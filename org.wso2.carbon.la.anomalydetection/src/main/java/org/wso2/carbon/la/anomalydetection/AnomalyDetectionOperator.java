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


package org.wso2.carbon.la.anomalydetection;

import org.wso2.carbon.la.anomalydetection.Operations.CsvDataWriter;
import org.wso2.carbon.la.anomalydetection.Operations.DataReader;
import org.wso2.carbon.la.anomalydetection.Operations.FeatureExtractor;
import org.wso2.carbon.la.commons.domain.RecordBean;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

/**
 * This class gets the call from the Rest-API
 * Depending on the request from the Rest-API this class either creates features or,
 * Starts a seperate JVM and runs the analysis on that JVM.
 */
public class AnomalyDetectionOperator {

    public void startFeatureEngineering() throws IOException {

        DataReader dataReader = new DataReader();
        List<RecordBean> recordBeans = dataReader.ReadData(0,108000);
        FeatureExtractor featurExtractor = new FeatureExtractor();
        List<List<RecordBean>> splittedRecordeBeansList = featurExtractor.splitRecordBeansIgnoreTimeStamp(recordBeans);
        List<Vector<Double>> featureVectorList = featurExtractor.createFeatureVectors(splittedRecordeBeansList);

        CsvDataWriter.writeCsvFile("/home/sameera/featureVectors.csv", featureVectorList);
    }

    public void startCheckingForAnomalies() throws IOException {
        System.out.println("===============Starting external JVM================");
        Process p = null;
        try {
            p = Runtime.getRuntime().exec("java -jar /home/sameera/Documents/sameera/WSO2Source/logAnalizer/LogAnomalyDetection/target/org.wso2.logAnalizer.AnomalyDetection-1.0-SNAPSHOT.jar");
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedInputStream bis = new BufferedInputStream(p.getInputStream());
        synchronized (p) {
            try {
                p.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(p.exitValue());
        int b=0;
        while((b=bis.read()) >0){

            System.out.print((char)b);
        }
        System.out.println("");
        System.out.println("==============Stopping analysis process==================");
    }


}