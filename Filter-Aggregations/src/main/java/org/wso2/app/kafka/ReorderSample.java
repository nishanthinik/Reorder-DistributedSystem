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
public class ReorderSample extends Thread {

    private static final Logger log = Logger.getLogger(ReorderSample.class);
    private volatile LinkedBlockingQueue<String> inputList1;
    private volatile LinkedBlockingQueue<String> inputList2;
    private volatile LinkedBlockingQueue<String> outputList;

    ReorderSample(LinkedBlockingQueue<String> inputList1, LinkedBlockingQueue<String> inputList2,
                  LinkedBlockingQueue<String> outputList) {
        this.inputList1 = inputList1;
        this.inputList2 = inputList2;
        this.outputList = outputList;

    }


    public void run() {
        try {

            float s1 = 0, s2 = 0;
            String event1 = "", event2 = "";
            String[] events1, events2, serialNo1, serialNo2;


            event1 = inputList1.take();
            events1 = event1.split(",");
            serialNo1 = events1[0].split(":");
            s1 = Float.parseFloat(serialNo1[2]);


            event2 = inputList2.take();
            events2 = event2.split(",");
            serialNo2 = events2[0].split(":");
            s2 = Float.parseFloat(serialNo2[2]);

            while (true) {
                float minNum;
                minNum = min(s1, s2);

                if (minNum == s1) {
                    outputList.put(event1);
                    event1 = inputList1.take();
                    events1 = event1.split(",");
                    serialNo1 = events1[0].split(":");
                    s1 = Float.parseFloat(serialNo1[2]);
                } else if (minNum == s2) {
                    outputList.put(event2);
                    event2 = inputList2.take();
                    events2 = event2.split(",");
                    serialNo2 = events2[0].split(":");
                    s2 = Float.parseFloat(serialNo2[2]);
                }


            }
        } catch (InterruptedException e1) {
            log.error("Error " + e1.getMessage(), e1);
        }
    }
}
