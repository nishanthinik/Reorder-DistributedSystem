#!/bin/bash
java -XX:+UnlockCommercialFeatures -Xmx8g -Xms8g -cp .:classs:target/classes:lib/log4j-1.2.17.jar:lib/slf4j-1.5.10.wso2v1.jar:lib/org.wso2.carbon.logging-4.4.9.jar:lib/guava-13.0.1.jar: org.wso2.output.test.TestOutput1