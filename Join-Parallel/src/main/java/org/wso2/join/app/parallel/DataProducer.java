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

package org.wso2.join.app.parallel;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * DataProducerMain Class for kafka.
 */
public class DataProducer extends Thread {
    private static volatile LinkedBlockingQueue<String> messagesList1 = new LinkedBlockingQueue<>();
    private static volatile LinkedBlockingQueue<String> messagesList2 = new LinkedBlockingQueue<>();

    public static void main(String[] args) {
        String topicName1 = "kafka_joins";
        String topicName2 = "kafka_join";

        DataGenerator dataGenerator = new DataGenerator(messagesList1);
        DataGenerator2 dataGenerator2 = new DataGenerator2(messagesList2);

        KafkaProducer kafkaProducer1 = new KafkaProducer(messagesList1, topicName1, 3);
        KafkaProducer kafkaProducer2 = new KafkaProducer(messagesList2, topicName2, 3);

        dataGenerator.start();
        dataGenerator2.start();

        kafkaProducer1.start();
        kafkaProducer2.start();
    }
}
