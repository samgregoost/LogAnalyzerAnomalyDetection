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

import org.wso2.carbon.la.commons.domain.RecordBean;

import java.sql.Time;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This class splits record beans, splits them according to time windows, and create
 * feature vector of dimension 7.
 *
 */
public class FeatureExtractor {

    /**
     * Comparator for arranging the record results in time.
     *
     */
    public static Comparator<RecordBean> RecordBeanComparator
            = new Comparator<RecordBean>() {

        public int compare(RecordBean recordBean1, RecordBean recordBean2) {
            String time1 ="";
            String time2="";
            try{
                time1 = (String)recordBean1.getValues().get("_timestamp");
            }catch(Exception e){

            }
            try{
                time2 = (String)recordBean2.getValues().get("_timestamp");
            }catch(Exception e){

            }

            if(time1 == null){
                List<String> items = Arrays.asList(((String) recordBean1.getValues().get("_message")).split("\\s*,\\s*"));
                time1=items.get(0);
            }
            if(time2 == null){
                List<String> items = Arrays.asList(((String)recordBean2.getValues().get("_message")).split("\\s*,\\s*"));
                time2=items.get(0);
            }

            return time1.compareTo(time2);
        }

    };

    /**
     * Splits the full record bean list in to a fixed number of lists
     * size has been reached.
     *
     */
    static <T> List<List<T>> chopped(List<T> list, final int L) {
        List<List<T>> parts = new ArrayList<List<T>>();
        final int N = list.size();
        for (int i = 0; i < N; i += L) {
            parts.add(new ArrayList<T>(
                            list.subList(i, Math.min(N, i + L)))
            );
        }
        return parts;
    }


    /**
     * Splits the full record bean list in to a fixed number of lists
     * size has been reached.
     *
     * @param recordBeans,  the record beans list that should be splitted
     */
    public List<List<RecordBean>> splitRecordBeansIgnoreTimeStamp(List<RecordBean> recordBeans){
        List<List<RecordBean>> splitRecordBeans = chopped(recordBeans, 10);
      //  System.out.println(splitRecordBeans); // prints "[[5, 3, 1], [2, 9, 5], [0, 7]]"
        return splitRecordBeans;
    }

    /**
     * Splits the full record bean list in to number of lists depending on the timestamp
     * size has been reached.
     *
     * @param recordBeans,  the record beans list that should be splitted
     */
    public List<List<RecordBean>> splitRecordBeans(List<RecordBean> recordBeans){

       recordBeans.sort(RecordBeanComparator);
        List<List<RecordBean>> totalList = new ArrayList();
        for (int i = 0; i < recordBeans.size(); i++){
            Time beginTime = getTimeValueFromRecordBean(recordBeans.get(i));
            if(beginTime != null){
                List<RecordBean> tempList = new ArrayList<>();
                tempList.add(recordBeans.get(i));
                for (int j = i+1; j < recordBeans.size(); j++){
                    Time currentTime = getTimeValueFromRecordBean(recordBeans.get(j));
                    if(currentTime != null){
                        long timeDifference = getTimeDifference(beginTime, currentTime);
                        if(timeDifference >= 2){
                            totalList.add(new ArrayList<>(tempList));
                            beginTime = currentTime;
                            tempList.clear();
                            tempList.add(recordBeans.get(j));
                        }else{
                            tempList.add(recordBeans.get(j));
                        }
                    }
                }
                break;
            }
        }
        return totalList;
    }

    /**
     * Gets the time value from each record bean
     *
     * @param recordBean,  the record beans that the time value should be extracted
     */
    private Time getTimeValueFromRecordBean(RecordBean recordBean){
        DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        Time timeValue = null;
        try {
           timeValue = new Time(formatter.parse((String)recordBean.getValues().get("_timestamp")).getTime());

            if (timeValue == null) {
                List<String> items = Arrays.asList(((String)recordBean.getValues().get("_message")).split("\\s*,\\s*"));
                timeValue = new Time(formatter.parse(items.get(0)).getTime());

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return timeValue;
    }

    /**
     * Get the average number of packets feature from the distribution of the record beans
     *
     * @param recordBeans,  the list of record beans, the feature should be extracted
     */
    public double averagePacketsperRequest(List<RecordBean> recordBeans){
        int sum = 0;
        double average = 0;

        for(int i = 0; i < recordBeans.size(); i++){
            try {
               String strPackets = (String)recordBeans.get(i).getValues().get("_totalpackets");

                if (strPackets == null) {
                    List<String> items = Arrays.asList(((String) recordBeans.get(i).getValues().get("_message")).split("\\s*,\\s*"));
                    sum = sum + Integer.parseInt(items.get(0));

                }else{
                    sum = sum + Integer.parseInt(strPackets);
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

        average = (float)sum/recordBeans.size();
        return average;
    }

    /**
     * Get the average bytes per request feature from the distribution of the record beans
     *
     * @param recordBeans,  the list of record beans, the feature should be extracted
     */
    public double averageBytesperRequest(List<RecordBean> recordBeans){
        int sum = 0;
        double average = 0;

        for(int i = 0; i < recordBeans.size(); i++){
            try {
                String strBytes = (String)recordBeans.get(i).getValues().get("_totalbytes");

                if (strBytes == null) {
                    List<String> items = Arrays.asList(((String) recordBeans.get(i).getValues().get("_message")).split("\\s*,\\s*"));
                    sum = sum + Integer.parseInt(items.get(0));

                }else{
                    sum = sum + Integer.parseInt(strBytes);
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

        average = (float)sum/recordBeans.size();
        return average;
    }

    /**
     * Get the packet entropy distribution feature from the distribution of the record beans
     *
     * @param recordBeans,  the list of record beans, the feature should be extracted
     */
    public double getPacketEntropyDistribution(List<RecordBean> recordBeans){
        ArrayList<String> ipArray = new ArrayList();
        float totalPackets = 0;
        for(int i = 0; i < recordBeans.size(); i++){
            ipArray.add((String)recordBeans.get(i).getValues().get("_sourceaddress"));
            String strPackets = (String)recordBeans.get(i).getValues().get("_totalpackets");
            totalPackets = totalPackets + Float.parseFloat(strPackets);
        }

        Set<String> uniqueIPArrayList = new HashSet(ipArray);
        double entropyValue = 0;
        for( int j = 0; j < uniqueIPArrayList.size(); j ++){
            String currentIP = (String)uniqueIPArrayList.toArray()[j];
            float currentPacketSum = 0;
            for(int i = 0; i < recordBeans.size(); i++){
                if(currentIP.equals((String)recordBeans.get(i).getValues().get("_sourceaddress"))){
                    String strPackets = (String)recordBeans.get(i).getValues().get("_totalpackets");
                    currentPacketSum = currentPacketSum + Float.parseFloat(strPackets);
                }
            }
            entropyValue = entropyValue - (currentPacketSum/totalPackets)*Math.log((currentPacketSum/totalPackets)) / Math.log(2);
        }

        return entropyValue;
    }

    /**
     * Get the bytes entropy distribution feature from the distribution of the record beans
     *
     * @param recordBeans,  the list of record beans, the feature should be extracted
     */
    public double getByteEntropyDistribution(List<RecordBean> recordBeans){
        ArrayList<String> ipArray = new ArrayList();
        float totalBytes = 0;
        for(int i = 0; i < recordBeans.size(); i++){
            ipArray.add((String)recordBeans.get(i).getValues().get("_sourceaddress"));
            String strPackets = (String)recordBeans.get(i).getValues().get("_totalbytes");
            totalBytes = totalBytes + Float.parseFloat(strPackets);
        }

        Set<String> uniqueIPArrayList = new HashSet(ipArray);
        double entropyValue = 0;
        for( int j = 0; j < uniqueIPArrayList.size(); j ++){
            String currentIP = (String)uniqueIPArrayList.toArray()[j];
            float currentBytesSum = 0;
            for(int i = 0; i < recordBeans.size(); i++){
                if(currentIP.equals((String)recordBeans.get(i).getValues().get("_sourceaddress"))){
                    String strPackets = (String)recordBeans.get(i).getValues().get("_totalbytes");
                    currentBytesSum = currentBytesSum + Float.parseFloat(strPackets);
                }
            }
            entropyValue = entropyValue - (currentBytesSum/totalBytes)*Math.log((currentBytesSum/totalBytes)) / Math.log(2);
        }

        return entropyValue;
    }

    /**
     * Get the labels from the record beans
     *
     * @param recordBeans,  the list of record beans, the label should be extracted
     */
    public double getLabel(List<RecordBean> recordBeans){
        for(int i = 0; i < recordBeans.size(); i++){
            if(org.apache.commons.lang3.StringUtils.containsIgnoreCase((String)recordBeans.get(i).getValues().get("_label"), "Botnet")){
                return 1;
            }
        }
        return 0;
    }

    /**
     * Get the protocol ratios features from the distribution of the record beans
     *
     * @param recordBeans,  the list of record beans, the feature should be extracted
     */
    private Vector<Double> getProtocolRatios(List<RecordBean> recordBeans){
        int icmpCount = 0;
        int udpCount = 0;
        int tcpCount = 0;
        for(int i = 0; i < recordBeans.size(); i++){

            if("icmp".equalsIgnoreCase((String)recordBeans.get(i).getValues().get("_protocol"))){
                icmpCount++;
            }else if("tcp".equalsIgnoreCase((String)recordBeans.get(i).getValues().get("_protocol"))){
                udpCount++;
            }else if("udp".equalsIgnoreCase((String)recordBeans.get(i).getValues().get("_protocol"))){
                tcpCount++;
            }
        }

        int totalCount = icmpCount + udpCount + tcpCount;
        Vector<Double> ratioVector = new Vector<>(3);
        ratioVector.add((double)icmpCount/totalCount);
        ratioVector.add((double)tcpCount/totalCount);
        ratioVector.add((double)udpCount/totalCount);

        return ratioVector;

    }

    /**
     * Creates the feature vecotrs from the record beans
     *
     * @param splittedRecordBeansList,  the list of record beans, the feature vectors should be extracted
     */
    public List<Vector<Double>> createFeatureVectors(List<List<RecordBean>> splittedRecordBeansList){

        List<Vector<Double>> featureVectorList = new ArrayList<>();
        for(int i = 0; i < splittedRecordBeansList.size(); i++){
            Vector<Double> featureVector = new Vector<>(5);
            featureVector.add(averagePacketsperRequest(splittedRecordBeansList.get(i)));
            featureVector.add(averageBytesperRequest(splittedRecordBeansList.get(i)));
            featureVector.add(getPacketEntropyDistribution(splittedRecordBeansList.get(i)));
            featureVector.add(getByteEntropyDistribution(splittedRecordBeansList.get(i)));
            featureVector.addAll((getProtocolRatios(splittedRecordBeansList.get(i))));
            featureVector.add(getLabel(splittedRecordBeansList.get(i)));
            featureVectorList.add(featureVector);
        }

        return featureVectorList;
    }

    /**
     * Gets the time differences from two time instants
     *
     * @param timeValue1,  the time value 1
     * @param timeValue2,  the time value 2
     */
    private long getTimeDifference(Time timeValue1, Time timeValue2){
        return (timeValue2.getTime()-timeValue1.getTime())/1000;
    }

}
