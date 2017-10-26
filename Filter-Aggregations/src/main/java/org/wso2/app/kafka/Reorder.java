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
public class Reorder extends Thread {

    private static final Logger log = Logger.getLogger(Reorder.class);
    private volatile LinkedBlockingQueue<String> inputList1;
    private volatile LinkedBlockingQueue<String> inputList2;
    private volatile LinkedBlockingQueue<String> inputList3;
    private volatile LinkedBlockingQueue<String> inputList4;
    private volatile LinkedBlockingQueue<String> outputList;
    private String x;
    private Integer listNum;

    Reorder(LinkedBlockingQueue<String> inputList1, LinkedBlockingQueue<String> inputList2,
            LinkedBlockingQueue<String> outputList, Integer listNum, String x) {
        this.inputList1 = inputList1;
        this.inputList2 = inputList2;
        this.outputList = outputList;
        this.x = x;
        this.listNum = listNum;

    }

    Reorder(LinkedBlockingQueue<String> inputList1, LinkedBlockingQueue<String> inputList2,
            LinkedBlockingQueue<String> inputList3,
            LinkedBlockingQueue<String> outputList, Integer listNum, String x) {
        this.inputList1 = inputList1;
        this.inputList2 = inputList2;
        this.inputList3 = inputList3;
        this.outputList = outputList;
        this.x = x;
        this.listNum = listNum;

    }

    Reorder(LinkedBlockingQueue<String> inputList1, LinkedBlockingQueue<String> inputList2,
            LinkedBlockingQueue<String> inputList3, LinkedBlockingQueue<String> inputList4,
            LinkedBlockingQueue<String> outputList, Integer listNum, String x) {
        this.inputList1 = inputList1;
        this.inputList2 = inputList2;
        this.inputList3 = inputList3;
        this.inputList4 = inputList4;
        this.outputList = outputList;
        this.x = x;
        this.listNum = listNum;

    }


    public void run() {
        try {

            float s1 = 0, s2 = 0, s3 = 0, s4 = 0;
            String event1 = "", event2 = "", event3 = "", event4 = "";
            String[] events1, events2, serialNo1, serialNo2, events3, events4, serialNo3, serialNo4;


            event1 = inputList1.take();
            events1 = event1.split(",");
            serialNo1 = events1[0].split(":");
            s1 = Float.parseFloat(serialNo1[2]);


            event2 = inputList2.take();
            events2 = event2.split(",");
            serialNo2 = events2[0].split(":");
            s2 = Float.parseFloat(serialNo2[2]);

            if (listNum >= 3) {

                event3 = inputList3.take();
                events3 = event3.split(",");
                serialNo3 = events3[0].split(":");
                s3 = Float.parseFloat(serialNo3[2]);
                if (listNum == 4) {

                    event4 = inputList4.take();
                    events4 = event4.split(",");
                    serialNo4 = events4[0].split(":");
                    s4 = Float.parseFloat(serialNo4[2]);
                }
            }

            while (true) {
                float minNum1, minNum2, minNum;
                if (listNum == 4) {
                    minNum1 = min(s1, s2);
                    minNum2 = min(s3, s4);
                    minNum = min(minNum1, minNum2);
                } else if (listNum == 3) {
                    minNum1 = min(s1, s2);
                    minNum = min(minNum1, s3);
                } else {
                    minNum = min(s1, s2);
                }

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
                } else if (listNum >= 3) {
                    if (minNum == s3) {
                        outputList.put(event3);
                        event3 = inputList3.take();
                        events3 = event3.split(",");
                        serialNo3 = events3[0].split(":");
                        s3 = Float.parseFloat(serialNo3[2]);

                    } else if (listNum == 4) {
                        if (minNum == s4) {
                            outputList.put(event4);
                            event4 = inputList4.take();
                            events4 = event4.split(",");
                            serialNo4 = events4[0].split(":");
                            s4 = Float.parseFloat(serialNo4[2]);
                        }
                    } else {

                        log.info("hi");
                    }
                }
            }
        } catch (InterruptedException e1) {
            log.error("Error " + e1.getMessage(), e1);
        }
    }
}
