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

package org.wso2.carbon.la.anomalydetection.Operations;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

/**
 * This class writes the engineered features in to a CSV file
 *
 */
public class CsvDataWriter {

    //Delimiter used in CSV file
    private static final String COMMA_DELIMITER = ",";
    private static final String NEW_LINE_SEPARATOR = "\n";

    /**
     * Writes the created features to the CSV file
     * @param fileName the file name of the CSV file
     * @param featureVectorList, the feature vectors that should be written to the CSV file.
     */
    public static void writeCsvFile(String fileName, List<Vector<Double>> featureVectorList) {

        FileWriter fileWriter = null;

        try {
            fileWriter = new FileWriter(fileName);
            for (Vector<Double> featureVector : featureVectorList) {
                fileWriter.append(String.valueOf(featureVector.get(0)));
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(String.valueOf(featureVector.get(1)));
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(String.valueOf(featureVector.get(2)));
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(String.valueOf(featureVector.get(3)));
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(String.valueOf(featureVector.get(4)));
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(String.valueOf(featureVector.get(5)));
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(String.valueOf(featureVector.get(6)));
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(String.valueOf(featureVector.get(7)));
                fileWriter.append(NEW_LINE_SEPARATOR);
            }

            System.out.println("Features were written successfully !!!");

        } catch (Exception e) {
            System.out.println("Error in CsvFileWriter !!!");
            e.printStackTrace();
        } finally {

            try {
                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e) {
                System.out.println("Error while flushing/closing fileWriter !!!");
                e.printStackTrace();
            }

        }
    }

}

