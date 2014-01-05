package com.labs.watermelon.gulp;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class FileReaderThread implements Runnable {
        
        private String file = null;
        private int readChunkSize;
        private InputQueue in = null;
        
        public FileReaderThread(String file, int readChunkSize, InputQueue in){
                this.file = file;
                this.readChunkSize = readChunkSize;
                this.in = in;
        }

        public void run() {
                try {                        
                        @SuppressWarnings("resource")
                        FileInputStream f = new FileInputStream(file);
                        FileChannel ch = f.getChannel( );
                        byte[] barray = new byte[readChunkSize];
                        ByteBuffer bb = ByteBuffer.wrap( barray );
                        int bytesRead = 0;
                        while((bytesRead =ch.read(bb)) != -1) {
                                if(bytesRead!=readChunkSize){
                                    synchronized(in) {
                                    	//System.out.println("Small chunk: " + new String(Arrays.copyOfRange(barray, 0, bytesRead)));
                                        in.getInQ().put(new String(Arrays.copyOfRange(barray, 0, bytesRead)));
                                        in.notify();
                                    }
                                }
                                else {
                                        synchronized(in) {
                                            in.getInQ().put(new String(barray));
                                            in.notify();
                                    }
                                }
                                
                            bb.clear( );
                        }
                        in.setStatus("done");                        
                        
                } 
                catch (FileNotFoundException e) {System.err.println("Could not open the file, FIle Not Found Exception");}
                catch (IOException e) {System.err.println("Could not read the file, IO Exception");}
                catch (InterruptedException  e) {System.err.println("Could not read the file, Interupted Exception");}
                
        }

}
