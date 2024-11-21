package com.example;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
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
    private transient Connection connection;
    private PreparedStatement insertStatement;

    // HashMap to store train ETAs
    private Map<String, Instant[]> etaMap;

    public ETAUpdater() {
        this.etaMap = new HashMap<>();
    }

    private Connection initializeConnection() {
        try {
            // Load the MySQL driver (optional, depending on your JDBC version)
            Class.forName("com.mysql.cj.jdbc.Driver");
            
            // Create the connection
            return DriverManager.getConnection(
                "jdbc:mysql://mysql:3306/mydatabase", // Use the appropriate URL
                "myuser",    // Username
                "mypassword" // Password
            );
        } catch (ClassNotFoundException e) {
            System.err.println("MySQL JDBC Driver not found.");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Connection to MySQL database failed.");
            e.printStackTrace();
        }
        return null; // Return null if the connection cannot be established
    }
    


    public void recordDelay(String runNumber, String nextStation, Instant originalEta, Instant latestEta) {
        String trainLine = deriveTrainLine(runNumber);
        Integer trainDirection = getDirection(runNumber);
        CtaTrainLines CtaTrainLine = new CtaTrainLines();
        String[] stationOrder = CtaTrainLine.getStationOrder();
        System.out.println(nextStation);
        Integer nextStationIndex = Arrays.asList(stationOrder).indexOf(nextStation);
        Integer prevStationIndex = nextStationIndex + trainDirection;
        String prevStation = null;
        String endStation = null;

        if (trainDirection == 1) {
            endStation = stationOrder[0];
        }
        else if (trainDirection == -1) {
            endStation = stationOrder[stationOrder.length - 1];
        }
        if (prevStationIndex > -1 && prevStationIndex < stationOrder.length) {
            prevStation = stationOrder[prevStationIndex];
        }

        long delayInMinutes = ChronoUnit.MINUTES.between(originalEta, latestEta);
        Timestamp originalEtaTs = Timestamp.from(originalEta);
        Timestamp latestEtaTs = Timestamp.from(latestEta);

        if (delayInMinutes < 0) {
            System.err.println("Delay in minutes is negative: " + delayInMinutes);
            return;
        }
    
        // Initialize the database connection
        try (Connection connection = initializeConnection()) {
            String sql = "INSERT INTO train_delays"
            + "(train_line, run_number, day, previous_station, next_station, end_station, delay_in_minutes, original_eta, latest_eta)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                // Assuming you have a way to extract these details from trainInfo
                statement.setString(1, trainLine);  // Example: train line
                statement.setString(2, runNumber);      // Run number
                statement.setDate(3, Date.valueOf(LocalDate.now())); // Current date
                statement.setString(4, prevStation); // Previous station
                statement.setString(5, nextStation); // Next station
                statement.setString(6, endStation);
                statement.setFloat(7, delayInMinutes);  
                statement.setTimestamp(8, originalEtaTs);
                statement.setTimestamp(9, latestEtaTs);        // Delay in minutes
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

public void updateETA(TrainInfo trainInfo) {
    String key = trainInfo.getRn() + "_" + trainInfo.getNextStaNm();
    Instant[] etaTimes = etaMap.getOrDefault(key, new Instant[2]);

    if (etaTimes[0] == null) {
        etaTimes[0] = parseETA(trainInfo.getArrT());  // Set original ETA
    }

    etaTimes[1] = parseETA(trainInfo.getArrT());  // Update latest ETA
    etaMap.put(key, etaTimes);

    writeETAtoFile("/tmp/eta_output.txt");
}

    // Method to write etaMap to a file
    public void writeETAtoFile(String filePath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (Map.Entry<String, Instant[]> entry : etaMap.entrySet()) {
                String key = entry.getKey();
                Instant[] etaTimes = entry.getValue();
                writer.write(key + ": Original ETA = " + etaTimes[0] + ", Latest ETA = " + etaTimes[1]);
                writer.newLine();  // Move to the next line
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void appendToLogFile(String filePath, String logData) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) { // 'true' for appending
            writer.write(logData);
            writer.newLine(); // Move to the next line
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void checkAndRecordDelays(Set<String> currentMessageKeys) {
        // List to collect keys for removal
        List<String> keysToRemove = new ArrayList<>();

        // Iterate over the keys in etaMap
        for (String key : etaMap.keySet()) {
            // If the key is not in the current message keys, calculate the delay
            if (!currentMessageKeys.contains(key)) {
                Instant[] etaTimes = etaMap.get(key);
                // long delayInMinutes = ChronoUnit.MINUTES.between(etaTimes[0], etaTimes[1]);
                String[] parts = key.split("_");
                String runNumber = parts[0];

                String nextStation = parts[1];
                
                // Here you would call your method to record the delay
                recordDelay(runNumber, nextStation, etaTimes[0], etaTimes[1]);
                
                // Add the key to the list for removal
                keysToRemove.add(key);
            }
        }
        
    // Remove all the keys after iteration
    for (String key : keysToRemove) {
        etaMap.remove(key);
    }

    }
    

    private String deriveTrainLine(String runNumber) {
        return (runNumber.startsWith("8") || runNumber.startsWith("9")) ? "Red Line" : "Unknown Line";
    }

    private Integer getDirection(String runNumber) {
        if (runNumber.startsWith("8")) {
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

    public Map<String, Instant[]> getEtaMap() {
        return etaMap;
    }
}