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

package org.wso2.sample.partition;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;

/**
 * Sample Siddi App1.
 */
public class App {
    private static final Logger log = Logger.getLogger(App.class);

    public static void main(String[] args) {
//        String siddhiApp1 =
//                "@source(type='kafka', topic.list='kafka_join1', partition.no.list='0', threading.option='single"
//                        + ".thread', group.id=\"group\", bootstrap.servers='localhost:9092', @map(type='json'))\n"
//                        + "\n"
//                        + "define stream inputStream(SerialNo int, price double, deviceId string, weight double, "
//                        + "timeStamp long);\n"
//                        + "@sink(type='kafka', topic='kafka_result_join1', bootstrap.servers='localhost:9092', "
//                        + "partition.no='0', @map(type='json'))\n"
//                        + "define stream outputStream(SerialNo int, deviceId string, timeStamp long, price double, "
//                        + "weight double);\n"
//                        + "@info(name= \"query1\")\n"
//                        + "partition with (deviceId of inputStream) "
//                        + "begin"
//                        + "\tfrom inputStream\n"
//                        + "\tselect SerialNo, deviceId, time:timestampInMilliseconds() as timeStamp, price, weight "
//                        + "having ((SerialNo%2) == 0)\n"
//                        + "\tinsert into outputStream;"
//                        + "end";

        String siddhiApp1 =
                "@source(type='kafka', topic.list='kafka_join1', partition.no.list='0', threading.option='single"
                        + ".thread', group.id=\"group\", bootstrap.servers='localhost:9092', @map(type='json'))\n"
                        + "define stream inputStream(SerialNo int, price float, deviceId string, weight double, "
                        + "timeStamp "
                        + "long);\n"
                        + "\n"
                        + "@sink(type='kafka', topic='kafka_result_join1', bootstrap.servers='localhost:9092', "
                        + "partition"
                        + ".no='0', @map(type='json'))\n"
                        + "define stream outputStream(timeStamp long, price float);\n"
                        + "\n"
                        + "@info(name='query')\n"
                        + "from inputStream#log()\n"
                        + "select SerialNo, deviceId, timeStamp, price, weight\n"
                        + "insert into fooStream;\n"
                        + "from fooStream#throughput:throughput(timeStamp, price, \"both\")\n"
                        + "select time:timestampInMilliseconds() as timeStamp, price\n"
                        + "insert into outputStream;";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
        siddhiAppRuntime.start();
    }
}
