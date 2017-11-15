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
package org.wso2.app.kafka2;

//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//
//import java.util.ArrayList;

import org.apache.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Read Final output.
 */
public class ReadData extends Thread {

    private static final Logger log = Logger.getLogger(ReadData.class);
    private volatile LinkedBlockingQueue<String> orderedList;
    private String m;

    ReadData(LinkedBlockingQueue<String> orderedList, String m) {
        this.m = m;
        this.orderedList = orderedList;
    }

    public void run() {
        try {
            while (true) {
                String x = orderedList.take();

                log.info(x + m);


            }

        } catch (InterruptedException e1) {
            log.error("Error " + e1.getMessage(), e1);
        }
    }
}
