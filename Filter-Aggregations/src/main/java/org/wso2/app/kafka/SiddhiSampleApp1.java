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


/**
 * Sample Siddi App.
 */
public class SiddhiSampleApp1 {
    private static final Logger log = Logger.getLogger(SiddhiSampleApp1.class);


    public static void main(String[] args) {
        String siddhiApp0 =
                "@source(type='kafka', topic.list='kafka_topic3', partition.no.list='0', threading.option='single"
                        + ".thread', group.id=\"group\", bootstrap.servers='localhost:9092', @map(type='json'))\n"
                        + "\n"
                        + "define stream inputStream(SerialNo int, price double, deviceId string, weight double, "
                        + "timeStamp long);\n"
                        + "@sink(type='kafka', topic='kafka_result_topic0', bootstrap.servers='localhost:9092', "
                        + "partition.no='0', @map(type='json'))\n"
                        + "define stream outputStream(SerialNo int, deviceId string, timeStamp long, price double, "
                        + "weight double);\n"
                        + "@info(name= \"query1\")\n"
                        + "from inputStream\n"
                        + "select SerialNo, deviceId, timeStamp, price, weight having ((SerialNo%2) == 0)\n"
                        + "insert into outputStream;";

        String siddhiApp1 =
                "@source(type='kafka', topic.list='kafka_topic3', partition.no.list='1', threading.option='single"
                        + ".thread', group.id=\"group\", bootstrap.servers='localhost:9092', @map(type='json'))\n"
                        + "\n"
                        + "define stream inputStream(SerialNo int, price double, deviceId string, weight double, "
                        + "timeStamp long);\n"
                        + "@sink(type='kafka', topic='kafka_result_topic1', bootstrap.servers='localhost:9092', "
                        + "partition.no='0', @map(type='json'))\n"
                        + "define stream outputStream(SerialNo int, deviceId string, timeStamp long, price double, "
                        + "weight double);\n"
                        + "@info(name= \"query1\")\n"
                        + "from inputStream\n"
                        + "select SerialNo, deviceId, timeStamp, price, weight having ((SerialNo%2) == 0)\n"
                        + "insert into outputStream;";

        String siddhiApp2 =
                "@source(type='kafka', topic.list='kafka_topic3', partition.no.list='2', threading.option='single"
                        + ".thread', group.id=\"group\", bootstrap.servers='localhost:9092', @map(type='json'))\n"
                        + "\n"
                        + "define stream inputStream(SerialNo int, price double, deviceId string, weight double, "
                        + "timeStamp long);\n"
                        + "@sink(type='kafka', topic='kafka_result_topic2', bootstrap.servers='localhost:9092', "
                        + "partition.no='0', @map(type='json'))\n"
                        + "define stream outputStream(SerialNo int, deviceId string, timeStamp long, price double, "
                        + "weight double);\n"
                        + "@info(name= \"query1\")\n"
                        + "from inputStream\n"
                        + "select SerialNo, deviceId, timeStamp, price, weight having ((SerialNo%2) == 0)\n"
                        + "insert into outputStream;";

        String siddhiApp3 =
                "@source(type='kafka', topic.list='kafka_topic3', partition.no.list='3', threading.option='single"
                        + ".thread', group.id=\"group\", bootstrap.servers='localhost:9092', @map(type='json'))\n"
                        + "\n"
                        + "define stream inputStream(SerialNo int, price double, deviceId string, weight double, "
                        + "timeStamp long);\n"
                        + "@sink(type='kafka', topic='kafka_result_topic3', bootstrap.servers='localhost:9092', "
                        + "partition.no='0', @map(type='json'))\n"
                        + "define stream outputStream(SerialNo int, deviceId string, timeStamp long, price double, "
                        + "weight double);\n"
                        + "@info(name= \"query1\")\n"
                        + "from inputStream\n"
                        + "select SerialNo, deviceId, timeStamp, price, weight having ((SerialNo % 2) == 0)\n"
                        + "insert into outputStream;";


        String siddhiAppMid =
                "@source(type='kafkaDistributed', topic.list='kafka_topic', partition.no.list='0,1,2,3', threading"
                        + ".option='single.thread', order.enabled = 'true', group.id=\"group\", bootstrap"
                        + ".servers='localhost:9092', @map(type='json'))\n"
                        + "\n"
                        + "define stream inputStream(SerialNo int, price double, deviceId string, weight double, "
                        + "timeStamp long);\n"
                        + "\n"
                        + "@sink(type='kafka', topic='kafka_result_topic', bootstrap.servers='localhost:9092', "
                        + "partition.no='0', @map(type='json'))\n"
                        + "define stream outputStream(SerialNo int, deviceId string, timeStamp long, price double, "
                        + "weight double);\n"
                        + "@info(name = 'query1')\n"

                        + "from inputStream#window.lengthBatch(10)\n"
                        + "select SerialNo, deviceId,  timeStamp, sum(price) as "
                        + "price, weight\n"
                        + "insert into outputStream;";


        App app0 = new App(siddhiApp0);
        App app1 = new App(siddhiApp1);
        App app2 = new App(siddhiApp2);
        App app3 = new App(siddhiApp3);
        App appMid = new App(siddhiAppMid);

        app0.start();
        app1.start();
        app2.start();
        app3.start();
        appMid.start();


    }
}
