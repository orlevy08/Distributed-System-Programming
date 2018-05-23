package bgu.dsp.sarcasm.manager;

import bgu.dsp.sarcasm.common.Pair;

import java.io.File;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SyncFileManager {

    private Map<String, Pair<AtomicInteger, PrintWriter>> fileToTasks = new ConcurrentHashMap<>();
    private int totalNumOfFilesReceived = 0;
    private int totalNumOfFilesSent = 0;


    public SyncFileManager() {}

    public int getTotalNumOfFilesReceived() {
        return totalNumOfFilesReceived;
    }

    public int getTotalNumOfFilesSent() {
        return totalNumOfFilesSent;
    }

    public void addFile (String filename) throws Exception{
        File directory = new File(filename.split("/")[0]);
        directory.mkdir();
        PrintWriter writer = new PrintWriter(filename, "UTF-8");
        fileToTasks.put(filename, new Pair<>(new AtomicInteger(0), writer));
        totalNumOfFilesReceived++;
    }

    public void incrementNumOfTasks(String filename){
        fileToTasks.get(filename).getKey().incrementAndGet();
    }

    public void decrementNumOfTasks(String filename) {
        fileToTasks.get(filename).getKey().decrementAndGet();
    }

    public boolean writeToFile(String filename, String toWrite) throws Exception{
        Pair<AtomicInteger, PrintWriter> fileValues = fileToTasks.get(filename);
        PrintWriter writer = fileValues.getValue();
        synchronized(writer) {
            writer.println(toWrite);
            writer.flush();
        }
        if(fileValues.getKey().decrementAndGet() == 0) {
            writer.close();
            fileToTasks.remove(filename);
            totalNumOfFilesSent++;
            return true;
        }
        return false;
    }

    public boolean isEmpty(){
        return fileToTasks.isEmpty();
    }


}
