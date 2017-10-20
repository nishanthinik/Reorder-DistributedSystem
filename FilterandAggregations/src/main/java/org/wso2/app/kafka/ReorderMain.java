/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.app.kafka;

import org.apache.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.Math.min;

/**
 * Disorder handling Class using Seq number.
 */
public class ReorderMain extends Thread {

    private static final Logger log = Logger.getLogger(ReorderMain.class);
    private volatile LinkedBlockingQueue<String> inputList1;
    private volatile LinkedBlockingQueue<String> inputList2;
    private volatile LinkedBlockingQueue<String> outputList;
    private Integer listNum;

    ReorderMain(Integer listNum) {

        this.listNum = listNum;

    }


    public void run() {
       if(listNum%4 == 0){
           int t = (listNum/4);

       }




//        try {
//
//        } catch (InterruptedException e1) {
//            log.error("Error " + e1.getMessage(), e1);
//        }
    }
}
