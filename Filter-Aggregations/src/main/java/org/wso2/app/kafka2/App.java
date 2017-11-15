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

import org.apache.log4j.Logger;
import org.wso2.app.kafka2.utils.AlphaKSlackExtension;
import org.wso2.app.kafka2.utils.KSlackExtension;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;


/**
 * Sample Siddi App1.
 */
public class App extends Thread {
    private static final Logger log = Logger.getLogger(App.class);

    public static void main(String[] args) {
        String siddhiApp0 =
                "@source(type='kafka', topic.list='kafka_filters', partition.no.list='0', threading.option='single"
                        + ".thread', group.id=\"group\", bootstrap.servers='localhost:9092', @map(type='json'))\n"
                        + "\n"
                        + "define stream inputStream(SerialNo int, price double, deviceId string, weight double, "
                        + "timeStamp long);\n"
                        + "@sink(type='kafka', topic='kafka_result_topic', bootstrap.servers='localhost:9092', "
                        + "partition.no='0', @map(type='json'))\n"
                        + "define stream outputStream(SerialNo int, deviceId string, timeStamp long, price double, "
                        + "weight double);\n"
                        + "@info(name= \"query1\")\n"
                        + "from inputStream\n"
                        + "select SerialNo, deviceId, timeStamp, price, weight having ((SerialNo%2) == 0)\n"
                        + "insert into outputStream;";

        String siddhiApp1 =
                "@source(type='kafka', topic.list='kafka_filters', partition.no.list='1', threading.option='single"
                        + ".thread', group.id=\"group\", bootstrap.servers='localhost:9092', @map(type='json'))\n"
                        + "\n"
                        + "define stream inputStream(SerialNo int, price double, deviceId string, weight double, "
                        + "timeStamp long);\n"
                        + "@sink(type='kafka', topic='kafka_result_topic', bootstrap.servers='localhost:9092', "
                        + "partition.no='1', @map(type='json'))\n"
                        + "define stream outputStream(SerialNo int, deviceId string, timeStamp long, price double, "
                        + "weight double);\n"
                        + "@info(name= \"query1\")\n"
                        + "from inputStream\n"
                        + "select SerialNo, deviceId, timeStamp, price, weight having ((SerialNo%2) == 0)\n"
                        + "insert into outputStream;";

//        String siddhiApp2 =
//                "@source(type='kafka', topic.list='kafka_filter', partition.no.list='2', threading.option='single"
//                        + ".thread', group.id=\"group\", bootstrap.servers='localhost:9092', @map(type='json'))\n"
//                        + "\n"
//                        + "define stream inputStream(SerialNo int, price double, deviceId string, weight double, "
//                        + "timeStamp long);\n"
//                        + "@sink(type='kafka', topic='kafka_result_topic', bootstrap.servers='localhost:9092', "
//                        + "partition.no='0', @map(type='json'))\n"
//                        + "define stream outputStream(SerialNo int, deviceId string, timeStamp long, price double, "
//                        + "weight double);\n"
//                        + "@info(name= \"query1\")\n"
//                        + "from inputStream\n"
//                        + "select SerialNo, deviceId, timeStamp, price, weight having ((SerialNo%2) == 0)\n"
//                        + "insert into outputStream;";
//
//        String siddhiApp3 =
//                "@source(type='kafka', topic.list='kafka_filter', partition.no.list='3', threading.option='single"
//                        + ".thread', group.id=\"group\", bootstrap.servers='localhost:9092', @map(type='json'))\n"
//                        + "\n"
//                        + "define stream inputStream(SerialNo int, price double, deviceId string, weight double, "
//                        + "timeStamp long);\n"
//                        + "@sink(type='kafka', topic='kafka_result_topic', bootstrap.servers='localhost:9092', "
//                        + "partition.no='0', @map(type='json'))\n"
//                        + "define stream outputStream(SerialNo int, deviceId string, timeStamp long, price double, "
//                        + "weight double);\n"
//                        + "@info(name= \"query1\")\n"
//                        + "from inputStream\n"
//                        + "select SerialNo, deviceId, timeStamp, price, weight having ((SerialNo % 2) == 0)\n"
//                        + "insert into outputStream;";
//
//        String siddhiApp4 =
//                "@source(type='kafka', topic.list='kafka_filter', partition.no.list='4', threading.option='single"
//                        + ".thread', group.id=\"group\", bootstrap.servers='localhost:9092', @map(type='json'))\n"
//                        + "\n"
//                        + "define stream inputStream(SerialNo int, price double, deviceId string, weight double, "
//                        + "timeStamp long);\n"
//                        + "@sink(type='kafka', topic='kafka_result_topic', bootstrap.servers='localhost:9092', "
//                        + "partition.no='0', @map(type='json'))\n"
//                        + "define stream outputStream(SerialNo int, deviceId string, timeStamp long, price double, "
//                        + "weight double);\n"
//                        + "@info(name= \"query1\")\n"
//                        + "from inputStream\n"
//                        + "select SerialNo, deviceId, timeStamp, price, weight having ((SerialNo % 2) == 0)\n"
//                        + "insert into outputStream;";

//        String siddhiAppMid =
//                "@app:async "
//                        + "@source(type='kafka', topic.list='kafka_result_topic', partition.no.list='0', threading"
//                        + ".option='single"
//                        + ".thread', group.id=\"group\", bootstrap.servers='localhost:9092', @map(type='json'))\n"
//                        + "\n"
//                        + "define stream inputStream(SerialNo int, price double, deviceId string, weight double, "
//                        + "timeStamp long);\n"
//                        + "\n"
//                        + "@sink(type='kafka', topic='kafka_result', bootstrap.servers='localhost:9092', "
//                        + "partition.no='0', @map(type='json'))\n"
//                        + "define stream outputStream(SerialNo int, deviceId string, timeStamp long, price double);\n"
//                        + "\n"
//                        + "\n"
//                        + "@info(name = 'query1')\n"
//                        + "partition with( deviceId of inputStream)\n"
//                        + "begin\n"
//                        + "from inputStream#window.lengthBatch(10)\n"
//                        + "select SerialNo, deviceId,  timeStamp, sum(price) as price, weight\n"
//                        + "insert into fooStream;\n"
//                        + "end;\n"
//                        + "from fooStream#reorder:kslack(timeStamp, 1000l)"
//                        + "select SerialNo, deviceId,  timeStamp, price\n"
//                        + "insert into outputStream\n";




        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        SiddhiAppRuntime siddhiAppRuntime0 = siddhiManager.createSiddhiAppRuntime(siddhiApp0);
        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
//        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(siddhiApp2);
//        SiddhiAppRuntime siddhiAppRuntime3 = siddhiManager.createSiddhiAppRuntime(siddhiApp3);
//        SiddhiAppRuntime siddhiAppRuntime4 = siddhiManager.createSiddhiAppRuntime(siddhiApp4);
//        SiddhiAppRuntime siddhiAppRuntimeMid = siddhiManager.createSiddhiAppRuntime(siddhiAppMid);

        siddhiAppRuntime0.start();
        siddhiAppRuntime1.start();
//        siddhiAppRuntime2.start();
//        siddhiAppRuntime3.start();
//        siddhiAppRuntime4.start();
//        siddhiAppRuntimeMid.start();
    }
}
