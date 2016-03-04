/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import Operations.CsvFileReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class trains a machine learning model and tests it on a set of features
 *
 *
 */
public class AnomalyDetectionMachineLearner {


    /**
     * The main class which initializes the procedure
     *
     * @return void
     */
    public static void main(String[] args) {
        try{
            SparkConf conf = new SparkConf().setAppName("anomalyDetection").setMaster("local");
            JavaSparkContext sc = new JavaSparkContext(conf);
            analyzeData("/home/sameera/trainedLogisticModel", "/home/sameera/test1.csv", sc);
        }catch(Exception e){
            System.out.println(e);
            e.printStackTrace();
        }
    }

    /**
     * Analyzes the data and gives the prediction output
     *
     * @param modelPath, the local directory path of the trained model
     * @param dataPath, the local directory path of feature CSV file
     * @param sc, the javasparkcontext of this JVM
     *
     * @return void
     */
    private static void analyzeData(String modelPath, String dataPath, JavaSparkContext sc){
        List<LabeledPoint> inputData = CsvFileReader.readCsvFile(dataPath);
        JavaRDD<LabeledPoint> dataRDD = sc.parallelize(inputData);
        JavaRDD<LabeledPoint>[] trainingDataRDD = dataRDD.randomSplit(new double[]{0.7,0.3});// dataRDD.sample(false, 0.6, 11L);

        outputFeatureAverages(inputData);
        /*// Run training algorithm to build the model.
             int numIterations = 5000;
            final SVMModel model = SVMWithSGD.train(trainingDataRDD[0].rdd(), numIterations);
            model.clearThreshold();
            model.setThreshold(-300000.417074087);*/

        Integer numClasses = 2;
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;

// Train a DecisionTree model for classification.
        final DecisionTreeModel model = DecisionTree.trainClassifier(trainingDataRDD[0], numClasses,
                categoricalFeaturesInfo, impurity, maxDepth, maxBins);

        //Load the logistic regression model.
     //   final LogisticRegressionModel model = LogisticRegressionModel.load(sc.sc(), modelPath);
        JavaRDD<Tuple2<Object, Object>> scoreAndLabels = trainingDataRDD[1].map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double score = model.predict(p.features());
                        return new Tuple2<Object, Object>(score, p.label());
                    }
                }
        );

        List<Tuple2<Object,Object>> scoresLabels = scoreAndLabels.toArray();

        int falseCount = 0;
        int truePositiveCount = 0;
        int trueNegativeCount = 0;
        int falsePositiveCount = 0;
        int falseNegativeCount = 0;

        for(Tuple2<Object,Object> tuple : scoresLabels){
            double score = (Double)tuple._1;
            double label = (Double)tuple._2;

            if(score == 0 && label==0){
                trueNegativeCount++;
            }else if(score == 1.0 && label==0){
                falseCount++;
                falsePositiveCount++;
            }else if(score == 0 && label== 1.0){
                falseCount++;
                falseNegativeCount++;
            }else if(score == 1 && label==1){
                truePositiveCount++;
            }
        }

        System.out.println();
        System.out.println("====================Printing performance evaluators======================");

        //Output the analysis
        System.out.println("True Positive Rate: " + (double) truePositiveCount / (truePositiveCount + falseNegativeCount));
        System.out.println("True Negative Rate: " + (double)trueNegativeCount/(trueNegativeCount + falsePositiveCount));
        System.out.println("False Positive Rate: " + (double)falsePositiveCount/(falsePositiveCount + trueNegativeCount));
        System.out.println("False Negative Rate: " + (double)falseNegativeCount/(falseNegativeCount+ truePositiveCount));
        System.out.println("Overall Accuracy: " + (1- (double)falseCount/scoreAndLabels.count()));

        System.out.println();
        BinaryClassificationMetrics metrics =
                new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
        System.out.println("Area under ROC: " + metrics.areaUnderROC());
        System.out.println();
    }

    /**
     * Prints the positive and negative feature averages seperately.
     *
     * @param inputData, the predicted output from the model.
     *
     * @return void
     */
    private static void outputFeatureAverages(List<LabeledPoint> inputData){
        double totalPositiveIcmpPrecentage = 0;
        double totalPositiveTcpPrecentage = 0;
        double totalPositiveUdpPrecentage = 0;
        double totalPositiveByteAverage = 0;
        double totalPositiveByteEntropy = 0;
        double totalPositivePacketEntropy = 0;
        double totalPositivePacketAverage = 0;
        int totalPositiveCount = 0;

        double totalNegativeIcmpPrecentage = 0;
        double totalNegativeTcpPrecentage = 0;
        double totalNegativeUdpPrecentage = 0;
        double totalNegativeByteAverage = 0;
        double totalNegativeByteEntropy = 0;
        double totalNegativePacketEntropy = 0;
        double totalNegativePacketAverage = 0;
        int totalNegativeCount = 0;

        for (LabeledPoint p: inputData){
            if(p.label() == 1.0){
                totalPositivePacketAverage += p.features().toArray()[0];
                totalPositiveByteAverage += p.features().toArray()[1];
                totalPositivePacketEntropy += p.features().toArray()[2];
                totalPositiveByteEntropy += p.features().toArray()[3];
                totalPositiveIcmpPrecentage += p.features().toArray()[4];
                totalPositiveTcpPrecentage += p.features().toArray()[5];
                totalPositiveUdpPrecentage +=  p.features().toArray()[6];
                totalPositiveCount++;
            }else{
                totalNegativePacketAverage += p.features().toArray()[0];
                totalNegativeByteAverage += p.features().toArray()[1];
                totalNegativePacketEntropy += p.features().toArray()[2];
                totalNegativeByteEntropy += p.features().toArray()[3];
                totalNegativeIcmpPrecentage += p.features().toArray()[4];
                totalNegativeTcpPrecentage += p.features().toArray()[5];
                totalNegativeUdpPrecentage +=  p.features().toArray()[6];
                totalNegativeCount++;
            }
        }

        System.out.println();
        System.out.println("===============Printing positive feature averages==============");
        System.out.println( "Positive packet average  " + totalPositivePacketAverage/totalPositiveCount);
        System.out.println( "Positive byte average  " + totalPositiveByteAverage/totalPositiveCount);
        System.out.println( "Positive packet entropy average  " + totalPositivePacketEntropy/totalPositiveCount);
        System.out.println( "Positive byte entropy average  " + totalPositiveByteEntropy/totalPositiveCount);
        System.out.println( "Positive icmp average  " + totalPositiveIcmpPrecentage/totalPositiveCount);
        System.out.println( "Positive tcp average  " + totalPositiveTcpPrecentage/totalPositiveCount);
        System.out.println( "Positive udp average  " + totalPositiveUdpPrecentage/totalPositiveCount);

        System.out.println();
        System.out.println("===============Printing negative feature averages==============");
        System.out.println( "Negative packet average  " + totalNegativePacketAverage/totalNegativeCount);
        System.out.println( "Negative byte average  " + totalNegativeByteAverage/totalNegativeCount);
        System.out.println( "Negative packet entropy average  " + totalNegativePacketEntropy/totalNegativeCount);
        System.out.println( "Negative byte entropy average  " + totalNegativeByteEntropy/totalNegativeCount);
        System.out.println( "Negative icmp average  " + totalNegativeIcmpPrecentage/totalNegativeCount);
        System.out.println( "Negative tcp average  " + totalNegativeTcpPrecentage/totalNegativeCount);
        System.out.println( "Negative udp average  " + totalNegativeUdpPrecentage/totalNegativeCount);

    }

    /**
     * Analyzes the data and gives the prediction output
     *
     * @param trainingData, the training dataset for the model.
     * @param modelPath, the local directory path of the trained model to be saved.
     * @param sc, the javasparkcontext of this JVM
     *
     * @return void
     */
    private static void trainAndSaveLogisticModel( JavaRDD<LabeledPoint> trainingData, JavaSparkContext sc, String modelPath){
        LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
                .setNumClasses(2)
                .run(trainingData.rdd());
        model.save(sc.sc(), modelPath + "/trainedLogisticModel");
    }
}
