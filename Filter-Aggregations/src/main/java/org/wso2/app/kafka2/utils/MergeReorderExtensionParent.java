/*
 * Copyright (c)  2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.app.kafka2.utils;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;

/**
 * The following code conducts reordering of an out-of-order event stream.
 * This implements the elimination of Duplicates.
 */

public class MergeReorderExtensionParent {

    private static final Logger log = Logger.getLogger(MergeReorderExtensionParent.class);
    private ExpressionExecutor serialNoExecutor;
    private double times = 0;
    private double lastSentSerialNo = 0.0;

    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        while (streamEventChunk.hasNext()) {

            ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<StreamEvent>(false);
            StreamEvent event = streamEventChunk.next();

//            synchronized (this) {

            streamEventChunk.remove();
            //We might have the rest of the events linked to this event forming a chain.

            double serialNo;
            Object[] data = event.getOutputData();
            if (data[0] == null) {
                times++;
                serialNo = lastSentSerialNo + (0.000001 * times);
            } else {
                serialNo = Double.parseDouble(serialNoExecutor.execute(event).toString());
                double difference = (serialNo - lastSentSerialNo);
                if (difference == 0) {
                    times++;
                    serialNo = lastSentSerialNo + (0.000001 * times);

                } else if (serialNo < lastSentSerialNo) {
                    times++;
                    serialNo = lastSentSerialNo + (0.000001 * times);
                } else {
                    times = 0;
                    lastSentSerialNo = serialNo;

                }
            }

            data[0] = serialNo;
            event.setOutputData(data);
            complexEventChunk.add(event);
            nextProcessor.process(complexEventChunk);


        }
    }
}
