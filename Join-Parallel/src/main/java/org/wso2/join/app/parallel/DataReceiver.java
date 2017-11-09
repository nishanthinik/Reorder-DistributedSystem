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
public class DataReceiver extends Thread {
//        private static volatile LinkedBlockingQueue<String> messagesList = new LinkedBlockingQueue<>();

    private static volatile LinkedBlockingQueue<String> messagesList0 = new LinkedBlockingQueue<>();
    private static volatile LinkedBlockingQueue<String> messagesList1 = new LinkedBlockingQueue<>();
    private static volatile LinkedBlockingQueue<String> messagesList2 = new LinkedBlockingQueue<>();
    private static volatile LinkedBlockingQueue<String> orderedList = new LinkedBlockingQueue<>();

    public static void main(String[] args) {

        KafkaReceiver kafkaReceiver0 = new KafkaReceiver(messagesList0, "kafka_result_join0", 0);
        KafkaReceiver kafkaReceiver1 = new KafkaReceiver(messagesList1, "kafka_result_join1", 0);
        KafkaReceiver kafkaReceiver2 = new KafkaReceiver(messagesList2, "kafka_result_join2", 0);
        kafkaReceiver0.start();
        kafkaReceiver1.start();
        kafkaReceiver2.start();

        Reorder reorder = new Reorder(messagesList0, messagesList1, messagesList2, orderedList, 3, "Final");
        reorder.start();
//        WriteToFile wf0 = new WriteToFile(messagesList0, "/1.txt");
//        wf0.start();
//        WriteToFile wf1 = new WriteToFile(messagesList1, "/2.txt");
//        wf1.start();
//        WriteToFile wf2 = new WriteToFile(messagesList2, "/3.txt");
//        wf2.start();
        WriteToFile wf = new WriteToFile(orderedList, "/b.txt");
        wf.start();

    }
}
