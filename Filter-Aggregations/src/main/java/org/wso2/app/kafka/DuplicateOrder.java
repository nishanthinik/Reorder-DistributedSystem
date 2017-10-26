package org.wso2.app.kafka;

import org.apache.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;

//import java.util.Iterator;

/**
 * Class for Kafka Producer.
 */
public class DuplicateOrder extends Thread {


    private static final Logger log = Logger.getLogger(DuplicateOrder.class);
    private volatile LinkedBlockingQueue<String> eventsList = new LinkedBlockingQueue<>();

    DuplicateOrder(LinkedBlockingQueue<String> eventsList) {
        this.eventsList = eventsList;

    }

    public void run() {
        String event1;
        String[] events1, serialNo1;
        float s1, cmp = 0f;
//        Iterator<String> it = eventsList.iterator();
        log.info("jjjjj");
        try {
            int times = 0;
            while (true) {

                event1 = eventsList.take();

                events1 = event1.split(",");
                serialNo1 = events1[0].split("\"");
                s1 = Float.parseFloat(serialNo1[4].replace(":", ""));

                float a = (s1 - cmp);
                if (a == 0) {
                    times++;
                    s1 = cmp + (0.01f * times);
                } else {
                    times = 0;
                    cmp = s1;
                }


                log.info(s1);
            }
        } catch (InterruptedException e1) {
            log.error("Error " + e1.getMessage(), e1);
        }
    }
}
