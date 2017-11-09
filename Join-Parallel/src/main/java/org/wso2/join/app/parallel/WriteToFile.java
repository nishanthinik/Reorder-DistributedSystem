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

//import com.google.common.base.Charsets;
import org.apache.log4j.Logger;

//import java.io.BufferedWriter;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.io.OutputStreamWriter;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Read Final output.
 */
public class WriteToFile extends Thread {

    private static final Logger log = Logger.getLogger(WriteToFile.class);
    private volatile LinkedBlockingQueue<String> orderedList;
    private String fileName;


    WriteToFile(LinkedBlockingQueue<String> orderedList, String fileName) {
        this.fileName = fileName;
        this.orderedList = orderedList;
    }

    public void run() {
//        String filePath = "/home/nishanthini/Project/MyProject/Reorder-DistributedSystem/Join-Parallel"
//                + fileName;
        try {

//            BufferedWriter bufferedWriter = null;
//            FileOutputStream fstream = new FileOutputStream(filePath);
//            bufferedWriter = new BufferedWriter(new OutputStreamWriter(fstream, Charsets.UTF_8));
            while (true) {
                String x = orderedList.take();

                log.info(x);
                //bufferedWriter.write(x + "\n");


            }

        } catch (InterruptedException e1) {
            log.error("Error " + e1.getMessage(), e1);
//        } catch (FileNotFoundException e) {
//            log.error("File Not Found" + e.getMessage(), e);
//        } catch (IOException e2) {
//            log.error("IO Exception " + e2.getMessage(), e2);
        }
    }
}
