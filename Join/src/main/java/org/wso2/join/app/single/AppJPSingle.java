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

package org.wso2.join.app.single;

import org.apache.log4j.Logger;
import org.wso2.join.app.single.utils.DuplicateExtension;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;


/**
 * Sample Siddi AppJoin.
 */
public class AppJPSingle {
    private static final Logger log = Logger.getLogger(AppJPSingle.class);

    public static void main(String[] args) {

        String siddhiApp1 =
                "@source(type='kafka', topic.list='kafka_topic', partition.no.list='0', threading"
                        + ".option='single.thread', group.id=\"group\", bootstrap.servers='localhost:9092', @map"
                        + "(type='json'))\n"

                        + "define stream inputStream(SerialNo int, timeStamp long, price double, weight double, "
                        + "deviceId string);\n"
                        + "\n"

                        + "@sink(type='kafka', topic='kafka_result_join', bootstrap.servers='localhost:9092', "
                        + "partition.no='0', @map(type='json'))\n"

                        + "define stream outputStream(SerialNo string, deviceId string, timeStamp long, timeStampTwo "
                        + "long, timeStampFinal long, SerialNoTwo int, timeStampA long, timeStampB long);\n"
                        + "\n"

                        + "@info(name = 'query1')\n"
                        + "partition with (deviceId of inputStream) "
                        + "begin"
                        + "\tfrom inputStream#window.time(10 sec) as a join inputStream#window.length(10) "
                        + "as b \n"
                        + "\ton (a.deviceId == (b.deviceId))\n"
                        + "\tselect convert(a.SerialNo, 'string') as SerialNo, b.deviceId, a.timeStamp, a.price "
                        + "as Price, b.timeStamp as timeStampTwo, b.SerialNo as SerialNoTwo, "
                        + "time:timestampInMilliseconds() as timeStampA\n"
                        + "\tinsert into#barStream;\n"
                        + "\tfrom#barStream\n"
                        + "\tselect SerialNo, deviceId, timeStamp, timeStampTwo, SerialNoTwo, timeStampA, "
                        + "time:timestampInMilliseconds() as timeStampB   "
                        + "\tinsert into fooStream;"
                        + "end;"

                        + "@info(name = 'query2')\n"
                        + "from fooStream#reorder:duplicate(SerialNo) "
                        + "select SerialNo, deviceId, timeStamp, timeStampTwo, time:timestampInMilliseconds() as "
                        + "timeStampFinal, SerialNoTwo, timeStampA, timeStampB "
                        + "insert into outputStream;";


        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:duplicate", DuplicateExtension.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
        siddhiAppRuntime.start();
    }
}
