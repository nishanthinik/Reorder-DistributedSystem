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


import java.util.concurrent.LinkedBlockingQueue;

/**
 * Class for Kafka Producer.
 */
public class KafkaClass {

    private static volatile LinkedBlockingQueue<String> eventsList0 = new LinkedBlockingQueue<>();
    private static volatile LinkedBlockingQueue<String> eventsList1 = new LinkedBlockingQueue<>();
    private static volatile LinkedBlockingQueue<String> eventsList2 = new LinkedBlockingQueue<>();
    private static volatile LinkedBlockingQueue<String> eventsList3 = new LinkedBlockingQueue<>();
    private static volatile LinkedBlockingQueue<String> eventsList4 = new LinkedBlockingQueue<>();
    //Ordered Events from eventsList0 and eventsList1
    private static volatile LinkedBlockingQueue<String> orderedList01 = new LinkedBlockingQueue<>();
    //Ordered Events from eventsList2 and eventsList3
    private static volatile LinkedBlockingQueue<String> orderedList23 = new LinkedBlockingQueue<>();
    //Data Received from Producer2
    private static volatile LinkedBlockingQueue<String> eventsList5 = new LinkedBlockingQueue<>();
    //Data Received from Producer3
    private static volatile LinkedBlockingQueue<String> eventsList6 = new LinkedBlockingQueue<>();
    private static volatile LinkedBlockingQueue<String> eventsList7 = new LinkedBlockingQueue<>();
    private static volatile LinkedBlockingQueue<String> eventsList8 = new LinkedBlockingQueue<>();

    private static volatile LinkedBlockingQueue<String> orderedListFinal = new LinkedBlockingQueue<>();
    private static volatile LinkedBlockingQueue<String> orderedListMidIn = new LinkedBlockingQueue<>();
    private static volatile LinkedBlockingQueue<String> orderedListMidOut = new LinkedBlockingQueue<>();

    public static void main(String[] args) {


        KafkaReceiver kr0 = new KafkaReceiver(eventsList0, "kafka_result_topic0", 0);
        KafkaReceiver kr1 = new KafkaReceiver(eventsList1, "kafka_result_topic1", 0);
        KafkaReceiver kr2 = new KafkaReceiver(eventsList2, "kafka_result_topic2", 0);
        KafkaReceiver kr3 = new KafkaReceiver(eventsList3, "kafka_result_topic3", 0);
        KafkaReceiver kr4 = new KafkaReceiver(eventsList4, "kafka_result_topic4", 0);
        kr0.start();
        kr1.start();
        kr2.start();
        kr3.start();
        kr4.start();

        Reorder rd1 = new Reorder(eventsList0, eventsList1, eventsList2, eventsList3,
                                  orderedList01, 4, "Order1");
        Reorder rd2 = new Reorder(eventsList4, orderedList01, orderedListMidIn, 2, " Final Order");

        rd1.start();
        rd2.start();

        KafkaProducer2 kafkaProducerMid = new KafkaProducer2(orderedListMidIn, "kafka_topic", 1);
        kafkaProducerMid.start();


        KafkaReceiver kafkaReceiverMid = new KafkaReceiver(orderedListMidOut, "kafka_result_topic", 0);
        kafkaReceiverMid.start();

        KafkaProducer2 kafkaProducerMid2 = new KafkaProducer2(orderedListMidOut, "kafka_mid", 3);
        kafkaProducerMid2.start();

//        Reorder reorder1 = new Reorder(eventsList0, eventsList1, orderedList01, 2, "01");
//        Reorder reorder2 = new Reorder(eventsList2, eventsList3, orderedList23, 2, "23");
//        reorder1.start();
//        reorder2.start();

//        KafkaProducer2 kp1 = new KafkaProducer2(orderedList01, "kafka_topic2", 1);
//        KafkaProducer2 kp2 = new KafkaProducer2(orderedList23, "kafka_topic1", 2);
//        KafkaProducer2 kp3 = new KafkaProducer2(eventsList4, "kafka_topic0", 1);
//        kp1.start();
//        kp2.start();
//        kp3.start();
//
//
//
        KafkaReceiver kr5 = new KafkaReceiver(eventsList5, "kafka_result0", 0);
        KafkaReceiver kr6 = new KafkaReceiver(eventsList6, "kafka_result1", 0);
        KafkaReceiver kr7 = new KafkaReceiver(eventsList7, "kafka_result2", 0);
//        KafkaReceiver kr8 = new KafkaReceiver(eventsList8, "kafka_result3", 0);
        kr5.start();
        kr6.start();
        kr7.start();
//        kr8.start();
//
//        Reorder reorder3 = new Reorder(eventsList5, eventsList6, orderedList23, 2, "  57");
//        Reorder reorder4 = new Reorder(orderedList23, eventsList7, orderedListFinal, 2, "68");
////        Reorder reorder5 = new Reorder(orderedList57, orderedList68, orderedListFinal,4, " Final Order");
        Reorder reorder = new Reorder(eventsList5, eventsList6, eventsList7,
                                      orderedListFinal, 3, " Final Order");
//
//        reorder3.start();
//        reorder4.start();
//////        reorder5.start();
        reorder.start();

//        ReadData rd = new ReadData(orderedList23, "Final------------23");
//        ReadData rd2 = new ReadData(orderedList01, "Final*************01");
        ReadData rdFinal = new ReadData(orderedListFinal, "----------------------Final");
//        rd.start();
//        rd2.start();
        rdFinal.start();
    }
}
