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

package org.wso2.sp.sample.kafka;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.sp.sample.kafka.utils.AlphaKSlackExtension;


/**
 * Sample Siddi App1.
 */
public class App {
    private static final Logger log = Logger.getLogger(App.class);
    // Pattern Query Single SP

    public static void main(String[] args) {
        String siddhiApp =
                "@source(type='kafka', topic.list='kafka_pattern', partition.no.list='0', threading"
                        + ".option='single.thread', group.id=\"group\", bootstrap.servers='localhost:9092', @map"
                        + "(type='json'))\n"
                        + "\n"
                        + "define stream inputStream(SerialNo int, price double, deviceId string, weight double, "
                        + "timeStamp long);\n"
                        + "\n"
                        + "@sink(type='kafka', topic='kafka_result_pattern', bootstrap.servers='localhost:9092', "
                        + "partition.no='0', @map(type='json'))\n"
                        + "define stream outputStream(SerialNo1 long, SerialNo2 long, deviceId1 string, "
                        + "finalPrice double, timeStamp long);\n"
                        + "\n"
                        + "\n"
                        + "@info(name = 'query1')\n"
                        + "from  every (e1=inputStream) -> e2=inputStream[e1.deviceId == deviceId and (e1.price + "
                        + "0.3) <= price] within 3 sec\n"
                        + "select convert (e1.SerialNo,'long') as SerialNo1, convert (e2.SerialNo,'long') as "
                        + "SerialNo2, e1.deviceId as "
                        + "deviceId1, "
                        + "e2.deviceId as deviceId2, e1.price as initialPrice, e2.price as finalPrice, e1.timeStamp\n"
                        + "insert into#tempStream;\n"
                        + "\n"
                        + "from#tempStream#reorder:akslack(SerialNo1, finalPrice, 10l)\n"
                        + "select SerialNo1, SerialNo2, deviceId1, finalPrice, timeStamp\n"
                        + "insert into outputStream;\n";
        SiddhiManager siddhiManager = new SiddhiManager();

        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });
        siddhiAppRuntime.start();
    }
}
