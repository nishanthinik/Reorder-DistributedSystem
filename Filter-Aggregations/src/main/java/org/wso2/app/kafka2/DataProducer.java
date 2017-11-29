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
 * DataProducerMain Class for kafka.
 */
public class DataProducer extends Thread {
    private static volatile LinkedBlockingQueue<String> messagesList1 = new LinkedBlockingQueue<>();

    public static void main(String[] args) throws InterruptedException {
        String topicName1 = "kafka_topic1";
        String topicName2 = "kafka_topic2";
        String topicName3 = "kafka_topic3";
        String[] topicName = {topicName1, topicName2, topicName3};

        DataGenerator dataLoader = new DataGenerator(messagesList1);
        Thread.sleep(100);
        KafkaProducer2 kafkaProducer1 = new KafkaProducer2(messagesList1, topicName, 0);
        dataLoader.start();
        kafkaProducer1.start();
    }
}
