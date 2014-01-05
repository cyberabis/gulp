package com.labs.watermelon.gulp;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class OutputQueue {

        private String status = "";
        //private Queue<Map<String, String>> outQ = new ConcurrentLinkedQueue<Map<String, String>>();
        private BlockingQueue<Map<String, String>> outQ = new ArrayBlockingQueue<Map<String, String>>(500);
        
        public String getStatus() {
                return status;
        }
        public void setStatus(String status) {
                this.status = status;
        }
        public BlockingQueue<Map<String, String>> getOutQ() {
                return outQ;
        }
        public void setOutQ(BlockingQueue<Map<String, String>> outQ) {
                this.outQ = outQ;
        }
        
}
