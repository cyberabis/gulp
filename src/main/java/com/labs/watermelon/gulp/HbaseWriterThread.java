package com.labs.watermelon.gulp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseWriterThread implements Runnable{
        
        private OutputQueue out;
        private String tableName;
        private String colFamily;
        private String colName;
        
        public HbaseWriterThread(OutputQueue out, String tableName, String colFamily, String colName){
                this.out = out;
                this.tableName = tableName;
                this.colFamily = colFamily;
                this.colName = colName;
        }

        public void run() {
                HTable table = null;
                try {
                        Configuration conf = HBaseConfiguration.create();
                        table = new HTable(conf, tableName);
                        table.setAutoFlush(false);
                } catch(IOException e){System.err.println("Error writing to Hbase");}
                
                while((!out.getStatus().equals("done"))||(out.getOutQ().size()!=0)){
                        if (out.getOutQ().size() == 0){
                                synchronized(out){
                                        System.err.println("Exception while waiting");
                                        try {
                                                try {
                                                        table.flushCommits();
                                                } catch (IOException e) {System.err.println("Exception while flushing");}
                                                out.wait();
                                        } catch (InterruptedException e) {System.err.println("Exception while waiting");}
                                }
                        }
                        Map<String, String> kv = out.getOutQ().remove();
                        List<Put> puts = new ArrayList<Put>();
                        for(String key: kv.keySet()){
                                Put put = new Put(Bytes.toBytes(key));
                                put.add(Bytes.toBytes(colFamily), Bytes.toBytes(colName), Bytes.toBytes(kv.get(key)));
                                puts.add(put);
                        }
                        try {
                                table.put(puts);
                        } catch (IOException e) {System.err.println("Error writing to Hbase");} 
                }
                try {
                        table.close();
                } catch (IOException e) {System.err.println("Exception while closing table");}
        }

}
