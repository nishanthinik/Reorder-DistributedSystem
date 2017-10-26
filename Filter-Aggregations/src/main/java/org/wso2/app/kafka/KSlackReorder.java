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

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Reorder using Kslack.
 */
public class KSlackReorder extends Thread {

    private static final Logger log = Logger.getLogger(KSlackReorder.class);
    private volatile LinkedBlockingQueue<String> eventsList;
    private volatile LinkedBlockingQueue<String> eventsList1;
    //In the beginning the K is zero.
    private long k = 0;
    //Used to track the greatest timestamp of tuples in the stream history.
    private long greatestTimestamp = 0;
    private TreeMap<Long, String> eventTreeMap = new TreeMap<Long, String>();
    private TreeMap<Long, String> expiredEventTreeMap = new TreeMap<Long, String>();
    private long maxK = Long.MAX_VALUE;
    private long timerDuration = -1L;
    private boolean expireFlag = false;
    private long lastSentTimeStamp = -1L;
    private long lastScheduledTimestamp = -1;
    private ReentrantLock lock = new ReentrantLock();

    KSlackReorder(LinkedBlockingQueue<String> eventsList, LinkedBlockingQueue<String> eventsList1) {
        super("KSlackReorder");
        this.eventsList = eventsList;
        this.eventsList1 = eventsList1;
    }

    public void run() {
        while (true) {

            try {
                String event = eventsList.take();
                String[] events = event.split(",");
                String[] time = events[7].split(":");
                String timeSt = time[1].replace("}", "");
                lock.lock();
                Long timestamp = Long.valueOf(timeSt);


                if (expireFlag) {
                    if (timestamp < lastSentTimeStamp) {
                        continue;
                    }
                }

                String eventList;
                eventList = eventTreeMap.get(timestamp);
                if (eventList == null) {
                    continue;
                }

                eventList = event;
                eventTreeMap.put(timestamp, eventList);

                if (timestamp > greatestTimestamp) {
                    greatestTimestamp = timestamp;
                    long minTimestamp = eventTreeMap.firstKey();
                    long timeDifference = greatestTimestamp - minTimestamp;

                    if (timeDifference > k) {
                        if (timeDifference < maxK) {
                            k = timeDifference;
                        } else {
                            k = maxK;
                        }
                    }

                    Iterator<Map.Entry<Long, String>> entryIterator = eventTreeMap.entrySet()
                            .iterator();

                    while (entryIterator.hasNext()) {
                        Map.Entry<Long, String> entry = entryIterator.next();
                        String list = expiredEventTreeMap.get(entry.getKey());


                        if (list != null) {
                            list = entry.getValue();
                        } else {
                            expiredEventTreeMap.put(entry.getKey(), entry.getValue());
                        }
                    }
                    eventTreeMap = new TreeMap<Long, String>();

                    entryIterator = expiredEventTreeMap.entrySet().iterator();
                    while (entryIterator.hasNext()) {
                        Map.Entry<Long, String> entry = entryIterator.next();

                        if (entry.getKey() + k <= greatestTimestamp) {
                            entryIterator.remove();
                            String timeEventList = entry.getValue();
                            lastSentTimeStamp = entry.getKey();

                            eventsList1.put(timeEventList);
                            log.info(timeEventList);


                        }
                    }
                }
            } catch (ArrayIndexOutOfBoundsException ec) {
                //                This happens due to user specifying an invalid field index.
                //                throw new SiddhiAppCreationException("The very first parameter must be an
                // Integer with a valid " +

                //                " field index (0 to (fieldsLength-1)).");
            } catch (Exception e) {
                log.error("Error when sending the messages", e);
            } finally {
                lock.unlock();
            }
        }
    }
}
