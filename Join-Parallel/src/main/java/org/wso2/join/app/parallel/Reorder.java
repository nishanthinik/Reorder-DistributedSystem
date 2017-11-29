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
package org.wso2.join.app.parallel;

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
            synchronized (this) {

                double s1, s2, s3 = 0, s4 = 0;
                String event1 = "", event2 = "", event3 = "", event4 = "";


                event1 = inputList1.take();
                s1 = Double.parseDouble((event1.split(","))[0].split(":")[2]);
                event2 = inputList2.take();
                s2 = Double.parseDouble((event2.split(","))[0].split(":")[2]);

                if (listNum >= 3) {
                    event3 = inputList3.take();
                    s3 = Double.parseDouble((event3.split(","))[0].split(":")[2]);

                    if (listNum == 4) {
                        event4 = inputList4.take();
                        s4 = Double.parseDouble((event4.split(","))[0].split(":")[2]);
                    }
                }

                while (true) {
                    double minNum;
                    if (listNum == 4) {
                        minNum = min(min(s1, s2), min(s3, s4));
//                        log.info(minNum + "---------- minNum1");
                    } else if (listNum == 3) {
                        minNum = min(min(s1, s2), s3);
//                        log.info(s1 + "---------- s1");
//                        log.info(s2 + "---------- s2");
//                        log.info(s3 + "---------- s3");
//                        log.info(minNum1 + "---------- minNum1");
//                        log.info(minNum + "---------- minNum--");
                    } else {
                        minNum = min(s1, s2);
//                        log.info(minNum + "---------- minNum3");
                    }

                    if (minNum == s1) {
                        log.info(event1);
//                        log.info(s1);
                        outputList.put(event1);
                        event1 = inputList1.take();
                        s1 = Double.parseDouble((event1.split(","))[0].split(":")[2]);
//
                    } else if (minNum == s2) {
//                        log.info(s2);
                        log.info(event2);
                        outputList.put(event2);
                        event2 = inputList2.take();
                        s2 = Double.parseDouble((event2.split(","))[0].split(":")[2]);
                    } else if (listNum >= 3) {
                        if (minNum == s3) {
//                            log.info(s3);
                            log.info(event3);
                            outputList.put(event3);
                            event3 = inputList3.take();
                            s3 = Double.parseDouble((event3.split(","))[0].split(":")[2]);

                        } else if (listNum == 4) {
                            if (minNum == s4) {
                                outputList.put(event4);
                                event4 = inputList4.take();
                                s4 = Double.parseDouble((event4.split(","))[0].split(":")[2]);
                            }
                        }
                    }
                }
            }
        } catch (InterruptedException e1) {
            log.error("Error " + e1.getMessage(), e1);
        }
    }
}
