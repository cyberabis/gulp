package com.labs.watermelon.gulp;

public class ParallelHbaseLoader {
    
    private String file;
    private int readChunkSize = 262144; //Default 256KB setting for File Ch with direct array access
    private InputQueue in = new InputQueue();
    private OutputQueue out = new OutputQueue();
    private String keyRegex;
    private String keyPattern;
    private String fileId;
    private String tableName;
    private String colFamily;
    private String colName;
    private int keyStartIndex;
    private int keyEndIndex;
    private boolean noRegex;
    
    public ParallelHbaseLoader(String file, int readChunkSize, String keyPattern, String keyRegex, String fileId, String tableName, String colFamily, String colName){
            this.file = file;
            this.readChunkSize = readChunkSize;
            this.keyPattern = keyPattern;
            this.keyRegex = keyRegex;
            this.fileId = fileId;
            this.tableName = tableName;
            this.colFamily = colFamily;
            this.colName = colName;
    }        
    
    public ParallelHbaseLoader(String file, String keyPattern, String fileId, String tableName, String colFamily, String colName){
            this.file = file;
            this.keyPattern = keyPattern;
            if (keyPattern.equals("dd/MMM/yyyy:HH:mm:ss"))
                    this.keyRegex = "(0?[1-9]|[12][0-9]|3[01])/([A-Z][a-z]{2})/((19|20)\\d\\d):[0-9]{2}:[0-9]{2}:[0-9]{2}";
            else if (keyPattern.equals("MM/dd/yyyy hh:mm:ss.SSS"))
                    this.keyRegex = "[0-1][0-9]/[0-3][0-9]/((19|20)\\d\\d) [0-2][0-9]:[0-6][0-9]:[0-6][0-9].[0-9]?[0-9]?[0-9]";
            this.fileId = fileId;
            this.tableName = tableName;
            this.colFamily = colFamily;
            this.colName = colName;
    }        
    
    public ParallelHbaseLoader(String file, String keyPattern, int keyStartIndex, int keyEndIndex, boolean noRegex, String fileId, String tableName, String colFamily, String colName){
        this.file = file;
        this.keyPattern = keyPattern;
        this.keyStartIndex = keyStartIndex;
        this.keyEndIndex = keyEndIndex;
        this.fileId = fileId;
        this.tableName = tableName;
        this.colFamily = colFamily;
        this.colName = colName;
        this.noRegex = noRegex;
}  
    
    public void load() {
            Thread readerThread = new Thread(new FileReaderThread(file, readChunkSize, in));
            Thread processorThread = new Thread(new FileProcessorThread(in, out, keyPattern, keyRegex, keyStartIndex, keyEndIndex, noRegex, readChunkSize, fileId));
            Thread writerThread = new Thread(new HbaseWriterThread(out, tableName, colFamily, colName));
            readerThread.start();
            processorThread.start();
            writerThread.start();
            
            try {
                    writerThread.join();
            } catch (InterruptedException e) {System.err.println("Error while waiting for thread to complete");}
            
    }
    
}
