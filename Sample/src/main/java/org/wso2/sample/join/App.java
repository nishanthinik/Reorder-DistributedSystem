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

package org.wso2.sample.join;

import org.apache.log4j.Logger;
import org.wso2.sample.join.utils.DuplicateExtension;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;


/**
 * Sample Siddi App1.
 */
public class App {
    private static final Logger log = Logger.getLogger(App.class);

    public static void main(String[] args) {
        String siddhiApp1 =
                "@source(type='kafka', topic.list='kafka_join1', partition.no.list='0', threading.option='single"
                        + ".thread', group.id=\"group\", bootstrap.servers='localhost:9092', @map(type='json'))\n"
                        + "\n"
                        + "define stream inputStream(SerialNo int, price double, deviceId string, weight double, "
                        + "timeStamp long);\n"

                        + "@source(type='kafka', topic.list='kafka_join2', partition.no.list='0', threading "
                        + ".option='single"
                        + ".thread', group.id=\"group\", bootstrap.servers='localhost:9092', @map(type='json'))\n"
                        + "\n"
                        + "define stream secondStream(SerialNo int, deviceId string, timeStamp long);\n"

                        + "@sink(type='kafka', topic='kafka_result_joins', bootstrap.servers='localhost:9092', "
                        + "partition.no='0', @map(type='json'))\n"
                        + "define stream outputStream(SerialNo string, deviceId string, timeStamp long, "
                        + "timeStampFinal long, timeStampOriginal long, SerialNoTwo int);\n"

                        + "@info(name= \"query1\")\n"
                        + "from inputStream#window.time(10 sec) as a join secondStream#window.length(10) as b\n"
                        + "on (a.deviceId == b.deviceId)\n"
                        + "select convert(a.SerialNo, 'string') as SerialNo, a.deviceId, "
                        + "time:timestampInMilliseconds() as timeStamp, a.timeStamp as timeStampOriginal, "
                        + "b.SerialNo as SerialNoTwo "
                        + "insert into fooStream;"

                        + "from fooStream#reorder:duplicate(SerialNo, 1000)"
//                        + "from fooStream "
                        + "select SerialNo, deviceId, timeStamp, time:timestampInMilliseconds() as timeStampFinal, "
                        + "timeStampOriginal, SerialNoTwo "
                        + "insert into outputStream; ";

//                        + " from outputStream#log() insert into test ";


        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:duplicate", DuplicateExtension.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
        siddhiAppRuntime.start();
    }
}
