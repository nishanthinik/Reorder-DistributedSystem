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

package org.wso2.app.kafka.utils;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import org.apache.log4j.Logger;
import org.joda.time.format.ISODateTimeFormat;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

//import com.google.common.io.CharStreams;

/**
 * Data loader for EDGAR log files.
 */
public class DataLoader extends Thread {
    private static final Logger log = Logger.getLogger(DataLoader.class);
    private JSONObject jsonMain = new JSONObject();
    private volatile LinkedBlockingQueue<String> messagesList;
    private String filePath = "/home/nishanthini/Project/MyProject/Reorder-DistributedSystem/Filter-Aggregations"
            + "/log20160901.csv";

    private Splitter splitter = Splitter.on(',');

    public DataLoader(LinkedBlockingQueue<String> messagesList) {
        super("Data Loader");
        this.messagesList = messagesList;
    }

    @Override
    public void run() {
        BufferedReader bufferedReader = null;
        try {
            String line;
            String[] header = new String[0];
            FileInputStream fstream = new FileInputStream(filePath);
            bufferedReader = new BufferedReader(new InputStreamReader(fstream, Charsets.UTF_8));


            float i = 0.0f;
            int sleepTime = 0;
            while ((line = bufferedReader.readLine()) != null) {

                if (sleepTime == 0) {
                    header = line.split(",");
                } else {

                    Iterator<String> dataStrIterator = splitter.split(line).iterator();
                    String ipAddress = dataStrIterator.next(); //
                    String date = dataStrIterator.next(); //yyyy-mm-dd
                    String time = dataStrIterator.next(); //hh:mm:ss
                    String zone = dataStrIterator.next(); //Zone is Apache log file zone
                    String cik = dataStrIterator.next(); //SEC Central Index Key (CIK) associated with the document
                    // requested
                    String accession = dataStrIterator.next(); //SEC document accession number associated with the
                    // document requested
                    String extention = dataStrIterator.next();
                    String code = dataStrIterator.next();
                    String size = dataStrIterator.next();
                    String idx = dataStrIterator.next();
                    String norefer = dataStrIterator.next();
                    String noagent = dataStrIterator.next();
                    String find = dataStrIterator.next();
                    String crawler = dataStrIterator.next();
                    String browser = dataStrIterator.next();
                    long timestamp = ISODateTimeFormat.dateTime().parseDateTime(date + "T" + time + ".000+0000")
                            .getMillis();

                    long timeStamp = System.nanoTime();
                    String[] dataItem =
                            new String[]{ipAddress, date, time, zone, cik, accession, extention, code, size, idx,
                                         norefer,
                                         noagent, find, crawler};


                    JSONObject jsonObj = new JSONObject();
                    jsonObj.put("SerialNo", i);
                    jsonObj.put("ipAddress", dataItem[0]);
                    jsonObj.put(header[1], dataItem[1]);
                    jsonObj.put("time", dataItem[2]);
                    jsonObj.put("zone", dataItem[3]);
                    jsonObj.put("cik", dataItem[4]);
                    jsonObj.put("accession", dataItem[5]);
                    jsonObj.put("extention", dataItem[6]);
                    jsonObj.put("code", dataItem[7]);
                    jsonObj.put("size", dataItem[8]);
                    jsonObj.put("idx", dataItem[9]);
                    jsonObj.put("norefer", dataItem[10]);
                    jsonObj.put("noagent", dataItem[11]);
                    jsonObj.put("find", dataItem[12]);
                    jsonObj.put("crawler", dataItem[13]);
                    jsonObj.put("timeStamp", timeStamp);


                    jsonMain.put("event", jsonObj);


                    try {
                        messagesList.put(jsonMain.toString());
                        Thread.sleep(10);
//                        if (sleepTime >= 1000) {
//
//                            sleepTime = 0;
//                        }
                    } catch (InterruptedException e1) {
                        log.error("Error " + e1.getMessage(), e1);
                    }
                }
                i++;
                sleepTime++;
            }
        } catch (FileNotFoundException e) {
            log.error("Error in accessing the input file. " + e.getMessage(), e);
        } catch (IOException e2) {
            log.error("Error. " + e2.getMessage(), e2);
        } catch (JSONException e3) {
            log.error("Error JSON . " + e3.getMessage(), e3);
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    log.error("Error in accessing the input file. " + e.getMessage(), e);
                }
            }
        }
    }
}
