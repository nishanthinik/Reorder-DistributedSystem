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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Class for Kafka Producer.
 */
public class KafkaProducer2 extends Thread {
    private static Logger log = Logger.getLogger(KafkaProducer2.class);
    private volatile LinkedBlockingQueue<String> messagesList;

    private String topicName;
    private int parts;

    KafkaProducer2(LinkedBlockingQueue<String> messagesList, String topicName, int parts) {
        this.messagesList = messagesList;
        this.topicName = topicName;
        this.parts = parts;
    }


    @Override
    public void run() {
        String type = "json";
        Properties props = new Properties();
        String broker = "localhost:9092";
        props.put("bootstrap.servers", broker);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        log.info("Initializing producer2");

        Producer<String, String> producer2 = new KafkaProducer(props);
        Random rnd = new Random();
        if (!"".equalsIgnoreCase("0001")) {

            String partitionNo;
            try {

                Iterator<String> it = messagesList.iterator();
                while (true) {
                    String message = messagesList.take();
                    Integer key;
                    if (parts > 0) {
                        key = rnd.nextInt(parts);
                    } else {
                        key = 0;
                    }

                    partitionNo = key.toString();

                        log.info(
                                "Sending " + message + " on topic: " + topicName + " to partition: " + partitionNo);
                    producer2.send(new ProducerRecord<>(topicName, key, "Producer", message));
                }


                // log.info("Kafka client finished sending events");
            } catch (Exception e) {
                log.error("Error when sending the messages", e);
            }
        }
        producer2.close();
    }
}
