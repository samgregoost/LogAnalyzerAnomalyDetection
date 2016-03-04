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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.analytics.datasource.commons.exception.AnalyticsException;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.la.commons.constants.LAConstants;
import org.wso2.carbon.la.commons.domain.QueryBean;
import org.wso2.carbon.la.commons.domain.RecordBean;
import org.wso2.carbon.la.core.impl.SearchController;

import java.util.ArrayList;
import java.util.List;

/**
 * This class Reads the data from Log analyzer DAS table
 *
 */
public class DataReader {

    private static final Log logger = LogFactory.getLog(DataReader.class);


    private SearchController searchController;

    public DataReader(){
        searchController = new SearchController();
    }

    /**
     * This method Reads the data from Log analyzer DAS table
     * size has been reached.
     *
     * @param setStart the  starting number of the DAS record
     * @param setLength, the desired result size.
     */
    public  List<RecordBean> ReadData(int setStart, int setLength){
        List<RecordBean> recordBeans =  new ArrayList<>();
        try {

            PrivilegedCarbonContext carbonContext = PrivilegedCarbonContext.getThreadLocalCarbonContext();
            String username = carbonContext.getUsername();
            QueryBean queryBean = new QueryBean();
            queryBean.setTableName(LAConstants.LOG_ANALYZER_STREAM_NAME);
            queryBean.setQuery("*:*");
            queryBean.setStart(setStart);
            queryBean.setLength(setLength);
            recordBeans = searchController.search(queryBean,username);


        } catch (AnalyticsException e1) {
            e1.printStackTrace();
        }
        return recordBeans;
    }

}
