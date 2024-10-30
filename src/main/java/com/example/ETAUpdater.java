package com.example;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.sql.Date;

import java.time.temporal.ChronoUnit;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

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
    


    public void recordDelay(TrainInfo trainInfo, float delayInMinutes) {
        if (trainInfo == null) {
            System.err.println("TrainInfo is null");
            return;
        }
        if (delayInMinutes < 0) {
            System.err.println("Delay in minutes is negative: " + delayInMinutes);
            return;
        }
    
        // Initialize the database connection
        try (Connection connection = initializeConnection()) {
            String sql = "INSERT INTO train_delays (train_line, run_number, day, previous_station, next_station, delay_in_minutes) VALUES (?, ?, ?, ?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                // Assuming you have a way to extract these details from trainInfo
                statement.setString(1, trainInfo.getDestSt());  // Example: train line
                statement.setString(2, trainInfo.getRn());      // Run number
                statement.setDate(3, Date.valueOf(LocalDate.now())); // Current date
                statement.setString(4, trainInfo.getNextStaId()); // Previous station
                statement.setString(5, trainInfo.getNextStpId()); // Next station
                statement.setFloat(6, delayInMinutes);          // Delay in minutes
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

public void updateETA(TrainInfo trainInfo) {
    String key = trainInfo.getRn() + "-" + trainInfo.getNextStpId();
    Instant[] etaTimes = etaMap.getOrDefault(key, new Instant[2]);

    if (etaTimes[0] == null) {
        etaTimes[0] = parseETA(trainInfo.getArrT());  // Set original ETA
    }

    etaTimes[1] = parseETA(trainInfo.getArrT());  // Update latest ETA
    etaMap.put(key, etaTimes);

    long delayInMinutes = ChronoUnit.MINUTES.between(etaTimes[0], etaTimes[1]);
    if (delayInMinutes > 0) {
        recordDelay(trainInfo, delayInMinutes);
    }

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