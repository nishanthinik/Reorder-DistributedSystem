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

package org.wso2.output.test;

import com.google.common.base.Charsets;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Data loader for EDGAR log files.
 */
public class TestOutput1 {
    private static final Logger log = Logger.getLogger(TestOutput1.class);

    private static String filePath = "/home/nishanthini/Project/SampleTh/Siddhi4_Sample1"
            + "/a.txt";


    public static void main(String[] args) {
        BufferedReader bufferedReader = null;
        try {
            String line;
            String[] events1, serialNo1;
            int s1;

            FileInputStream fstream = new FileInputStream(filePath);
            bufferedReader = new BufferedReader(new InputStreamReader(fstream, Charsets.UTF_8));


            int i = 0;
            int cmp = 0;
            while ((line = bufferedReader.readLine()) != null) {

                events1 = line.split(",");
                serialNo1 = events1[0].split(":");
                s1 = Integer.parseInt(serialNo1[2]);

                if (i == 0) {
                    log.info("Ok" + s1);
                    cmp = s1;
                } else if (cmp < s1) {
                    log.info("Ok" + s1);
                } else if (cmp > s1) {
                    log.info("Error Found. Not In order" + s1);
                    break;
                }
                i++;

            }
        } catch (FileNotFoundException e) {
            log.error("Error in accessing the input file. " + e.getMessage(), e);
        } catch (IOException e2) {
            log.error("Error in accessing the input file. " + e2.getMessage(), e2);
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    log.error("Error in accessing the input file. " + e.getMessage(), e);
                }
            }
        }
    }
}
