#!/bin/bash
java -XX:+UnlockCommercialFeatures -Xmx8g -Xms8g -cp .:target/Filter-Aggregations-1.0-SNAPSHOT-jar-with-dependencies.jar:lib/siddhi-core-4.0.0-alpha6.jar:lib/siddhi-io-kafka-4.0.3.jar:lib/siddhi-map-json-4.0.10.jar:lib/kafka-clients-0.9.0.1.jar org.wso2.app.kafka.SiddhiSampleApp1