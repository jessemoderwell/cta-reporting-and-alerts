package com.example;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set; 
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.sql.Date;
import java.sql.Timestamp;

import java.time.temporal.ChronoUnit;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import com.example.CtaTrainLines;

public class ETAUpdater implements Serializable {
    // HashMap to store train ETAs
    private static Map<String, Map<String, Instant[]>> etaMap = new HashMap<>();
    private Map<String, TrainInfo> trainInfoMap = new HashMap<>();

    public ETAUpdater() {
    }

    public void updateETA(TrainInfo trainInfo) {
        String trainLine = deriveTrainLine(trainInfo.getRn());
        String key = trainInfo.getRn() + "_" + trainInfo.getNextStaNm() + "_" + trainInfo.getTrDr();
        // System.out.println(trainLine);
    
        etaMap.putIfAbsent(trainLine, new HashMap<>());
        Map<String, Instant[]> trainLineMap = etaMap.get(trainLine);
        // System.out.println(trainLineMap);
    
        Instant[] etaTimes = trainLineMap.getOrDefault(key, new Instant[2]);
        // System.out.println("Current train:");
        // System.out.println(key);
        // System.out.println("Original eta:");
        // System.out.println(etaTimes[0]);
        // System.out.println("Latest eta:");
        // System.out.println(etaTimes[1]);
    
        if (etaTimes[0] == null) {
            etaTimes[0] = parseETA(trainInfo.getArrT());
        }
        etaTimes[1] = parseETA(trainInfo.getArrT());
    
        trainLineMap.put(key, etaTimes);
    
        // Update the trainInfoMap for later retrieval
        trainInfoMap.put(key, trainInfo);

        // System.out.println("Current state of etaMap:");
        // System.out.println(etaMap);
    
        writeETAtoFile("/tmp/eta_output.txt");
    }

    public void cleanupOldEntries(Set<String> currentMessageKeys) {
        // Iterate through each train line's data
            String sampleMessageKey = currentMessageKeys.iterator().next();
            String currentTrainLine = deriveTrainLine(sampleMessageKey.split("_")[0]);
            Map<String, Instant[]> trainLineData = etaMap.getOrDefault(currentTrainLine, new HashMap<>());
            // System.out.println("Train line before cleaning up:");
            // System.out.println(trainLineData);
    
            // Identify keys to remove
            List<String> keysToRemove = new ArrayList<>();
            for (String key : trainLineData.keySet()) {
                if (!currentMessageKeys.contains(key)) {
                    keysToRemove.add(key);
                }
            }
    
            // Remove outdated keys from etaMap and trainInfoMap
            for (String key : keysToRemove) {
                trainLineData.remove(key);
                trainInfoMap.remove(key);  // Also remove from the TrainInfo map
            }

            // System.out.println("Train line after cleaning up:");
            // System.out.println(trainLineData);
        }

    // Method to write etaMap to a file
    public void writeETAtoFile(String filePath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (Map.Entry<String, Map<String, Instant[]>> lineEntry : etaMap.entrySet()) {
                String trainLine = lineEntry.getKey();
                Map<String, Instant[]> trainLineData = lineEntry.getValue();
    
                for (Map.Entry<String, Instant[]> entry : trainLineData.entrySet()) {
                    String key = entry.getKey();
                    Instant[] etaTimes = entry.getValue();
                    writer.write(trainLine + " -> " + key + ": Original ETA = " + etaTimes[0] + ", Latest ETA = " + etaTimes[1]);
                    writer.newLine();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TrainInfo getTrainInfoByKey(String key) {
        return trainInfoMap.get(key); // Retrieve the stored TrainInfo object by key
    }

    public void appendToLogFile(String filePath, String logData) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) { // 'true' for appending
            writer.write(logData);
            writer.newLine(); // Move to the next line
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Set<String> identifyDisappearedTrains(Set<String> currentMessageKeys) {
        Set<String> disappearedKeys = new HashSet<>();
    
        // Iterate through each train line's data
        String sampleMessageKey = currentMessageKeys.iterator().next();
        String currentTrainLine = deriveTrainLine(sampleMessageKey.split("_")[0]);
        Map<String, Instant[]> trainLineData = etaMap.getOrDefault(currentTrainLine, new HashMap<>());
        System.out.println(trainLineData);
    
            for (String key : trainLineData.keySet()) {
                // System.out.println(key);
                // If a key in etaMap is not in the current message, mark it as disappeared
                if (!currentMessageKeys.contains(key)) {
                    System.out.println("Train has disappeared from message:");
                    System.out.println(key);
                    disappearedKeys.add(key);
                }
            }
        
        return disappearedKeys;
    }
    

    public static String deriveTrainLine(String runNumber) {
        if (runNumber.startsWith("8") || runNumber.startsWith("9")) {
            return "Red Line";
        }
        else if (runNumber.startsWith("7")) {
            return "Orange Line";
        } else if (runNumber.startsWith("0") || runNumber.startsWith("6")) {
            return "Green Line";
        } else if (runNumber.startsWith("4")) {
            return "Brown Line";
        } else if (runNumber.startsWith("1") || runNumber.startsWith("2")) {
            return "Blue Line";
        }
        return "Unknown Line";
    }

    public static Integer getDirection(String direction) {
        if (direction == "1") {
            return 1;
        }
        return -1;
    }

    private Instant parseETA(String eta) {
        // Modify the parsing logic to fit the format of your ETA timestamps
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
                .withZone(ZoneOffset.UTC);
        return Instant.from(formatter.parse(eta.replaceAll("[-T:.]", "")));
    }

    public static Map<String, Map<String, Instant[]>> getEtaMap() {
        return etaMap;
    }
}