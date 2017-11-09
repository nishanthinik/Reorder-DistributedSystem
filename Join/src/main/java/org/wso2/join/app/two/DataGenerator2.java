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
package org.wso2.join.app.two;


import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * A data generator which uses random numbers.
 */
public class DataGenerator2 extends Thread {
    private JSONObject jsonMain = new JSONObject();

    private volatile LinkedBlockingQueue<String> messagesList;
    private static final Logger log = Logger.getLogger(DataGenerator2.class);
    private boolean shutdownFlag = false;

    public DataGenerator2(LinkedBlockingQueue<String> messagesList) {
        super("Data Generator");
        this.messagesList = messagesList;
    }

    public void run() {
        int i = 1;
        Random rand = new Random();
        String[] listItem = new String[]{"a", "b", "c", "d", "e"};
        String id = listItem[rand.nextInt(4)];
        Object[] dataItem = new Object[]{i, System.currentTimeMillis(), id};

        while (i <= 5000000) {

            id = listItem[rand.nextInt(4)];
            dataItem[0] = i;
            dataItem[1] = System.currentTimeMillis();
            dataItem[2] = id;


            JSONObject jsonObj = new JSONObject();
            jsonObj.put("SerialNo", dataItem[0]);
            jsonObj.put("timeStamp", dataItem[1]);
            jsonObj.put("deviceId", dataItem[2]);



            jsonMain.put("event", jsonObj);


            try {
                messagesList.put(jsonMain.toString());
                        Thread.sleep(0);
//                        if (sleepTime >= 1000) {
//
//                            sleepTime = 0;
//                        }
            } catch (InterruptedException e1) {
                log.error("Error " + e1.getMessage(), e1);
            }
            i++;
        }
    }

    public void shutdown() {
        shutdownFlag = true;
    }
}
