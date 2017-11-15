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


import java.util.concurrent.LinkedBlockingQueue;

/**
 * Class for Kafka Consumer.
 */
public class KafkaConsumer {

    private static volatile LinkedBlockingQueue<String> eventsList0 = new LinkedBlockingQueue<>();

    public static void main(String[] args) {


        KafkaReceiver kr0 = new KafkaReceiver(eventsList0, "kafka_result_topic", 0);
        kr0.start();


        WriteToFile wf = new WriteToFile(eventsList0, "Final ");
        wf.start();
    }

}
