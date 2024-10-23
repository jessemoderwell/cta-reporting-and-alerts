package com.example;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ETAUpdater implements Serializable {

    // HashMap to store train ETAs
    private Map<String, long[]> etaMap;

    public ETAUpdater() {
        this.etaMap = new HashMap<>();
    }

    public void updateETA(TrainInfo trainInfo) {
        // Combine run number and next stop as a unique key
        String key = trainInfo.getRn() + "-" + trainInfo.getNextStpId();
        long[] etaTimes = etaMap.getOrDefault(key, new long[2]);

        if (etaTimes[0] == 0) {
            // Set the first element as the original ETA if it's the first time seeing this key
            etaTimes[0] = parseETA(trainInfo.getArrT());
        }
        
        // Always update the second element to the latest ETA
        etaTimes[1] = parseETA(trainInfo.getArrT());

        // Update the map
        etaMap.put(key, etaTimes);

        // Write the updated etaMap to a file after each update
        writeETAtoFile("/tmp/eta_output.txt");
    }

    // Method to write etaMap to a file
    public void writeETAtoFile(String filePath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (Map.Entry<String, long[]> entry : etaMap.entrySet()) {
                String key = entry.getKey();
                long[] etaTimes = entry.getValue();
                writer.write(key + ": Original ETA = " + etaTimes[0] + ", Latest ETA = " + etaTimes[1]);
                writer.newLine();  // Move to the next line
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private long parseETA(String eta) {
        // Parsing logic (convert eta string to an int, for example, minutes or seconds since epoch)
        // Replace with actual parsing logic for ETA
        return Long.parseLong(eta.replaceAll("[-T:.]", ""));
    }

    public Map<String, long[]> getEtaMap() {
        return etaMap;
    }
}