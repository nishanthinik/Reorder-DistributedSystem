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

package org.wso2.sample.partitionjoin;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * DataReceiver Main Class for kafka.
 */
public class DataReceiver extends Thread {
    private static volatile LinkedBlockingQueue<String> messagesList1 = new LinkedBlockingQueue<>();

    public static void main(String[] args) {
        KafkaReceiver kafkaReceiver = new KafkaReceiver(messagesList1, "kafka_result_joins", 0);
        kafkaReceiver.start();
        WriteToFile wf = new WriteToFile(messagesList1, "Final");
        wf.start();

    }
}
