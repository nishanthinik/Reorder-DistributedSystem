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

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Test client for Kafka Receiver.
 */
public class KafkaReceiver extends Thread {
    private static final Logger log = Logger.getLogger(KafkaReceiver.class);

    private volatile LinkedBlockingQueue<String> eventsList;
    private String topicName;
    private int partitionNo;
    private String pNo;


    KafkaReceiver(LinkedBlockingQueue<String> eventsList, String topicName, int partitionNo) {
        this.eventsList = eventsList;
        this.topicName = topicName;
        this.partitionNo = partitionNo;

    }

    public void run() {
        List<TopicPartition> partitionsList = new ArrayList<>();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group");
        props.put("session.timeout.ms", "30000");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        TopicPartition partition = new TopicPartition(topicName, partitionNo);
        partitionsList.add(partition);
        consumer.assign(partitionsList);

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(10);
            try {
                for (ConsumerRecord record : records) {
                    String event = record.value().toString();
//                    log.info(event);
                    eventsList.put(event);
                }
            } catch (InterruptedException e1) {
                log.error("Error " + e1.getMessage(), e1);
            }

            try {
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            } catch (CommitFailedException e) {
                log.error("Kafka commit failed for topic " + topicName, e);
            }
        }
    }
}
