#!/bin/bash
java -XX:+UnlockCommercialFeatures -Xmx8g -Xms8g -cp .:classs:target/classes:lib/kafka-clients-0.9.0.1.jar:lib/kafka_2.10-0.9.0.1.jar:lib/log4j-1.2.17.jar:lib/slf4j-1.5.10.wso2v1.jar:lib/org.wso2.carbon.logging-4.4.9.jar:lib/guava-13.0.1.jar:lib/json-20131018.jar:lib/joda-time-2.0.jar org.wso2.join.app.single.DataProducer localhost:9092 kafka_topic3 json 0001 log20160901 0
