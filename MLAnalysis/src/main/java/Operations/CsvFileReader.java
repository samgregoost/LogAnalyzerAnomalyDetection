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

package Operations;

import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class splits reads the feature vectors from the local storage
 *
 */
public class CsvFileReader {
    //Delimiter used in CSV file
    private static final String COMMA_DELIMITER = ",";

    //Feature index
    private static final int AVERAGE_NUMBER_OF_PACKETS = 0;
    private static final int AVERAGE_NUMBER_OF_BYTES = 1;
    private static final int PACKET_ENTROPY = 2;
    private static final int BYTE_ENTROPY = 3;
    private static final int ICMP_RATIO = 4;
    private static final int TCP_RATIO= 5;
    private static final int UDP_RATIO = 6;
    private static final int LABEL = 7;

    /**
     * Reads the feature CSV file and return it as a labeled point
     *
     * @param fileName, the file name of the saved feature vector filr
     *
     * @return list of labeled point generated from the CSV file.
     */
    public static List<LabeledPoint> readCsvFile(String fileName) {

        BufferedReader fileReader = null;
        List<LabeledPoint> inputData = new ArrayList<LabeledPoint>();

        try {
            String line;
            fileReader = new BufferedReader(new FileReader(fileName));
            //Read the file line by line starting from the second line
            while ((line = fileReader.readLine()) != null) {
                //Get all features available in line
                String[] tokens = line.split(COMMA_DELIMITER);
                if (tokens.length > 0) {
                    LabeledPoint dataPoint = new LabeledPoint((int)Double.parseDouble(tokens[LABEL]), Vectors.dense(Double.parseDouble(tokens[AVERAGE_NUMBER_OF_PACKETS]),Double.parseDouble(tokens[AVERAGE_NUMBER_OF_BYTES]),Double.parseDouble(tokens[PACKET_ENTROPY]),Double.parseDouble(tokens[BYTE_ENTROPY]),Double.parseDouble(tokens[ICMP_RATIO]),Double.parseDouble(tokens[TCP_RATIO]),Double.parseDouble(tokens[UDP_RATIO])));
                    inputData.add(dataPoint);
                }
            }
        }
        catch (Exception e) {
            System.out.println("Error in CsvFileReader");
            e.printStackTrace();
        } finally {
            try {
                fileReader.close();
            } catch (IOException e) {
                System.out.println("Error while closing fileReader !!!");
                e.printStackTrace();
            }
        }

        return inputData;

    }
}
