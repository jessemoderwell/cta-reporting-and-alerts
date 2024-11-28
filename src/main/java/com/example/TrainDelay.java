package com.example;

import java.sql.Timestamp;

public class TrainDelay {
    private String trainLine;
    private String runNumber;
    private String previousStation;
    private String nextStation;
    private String endStation;
    private long delayInMinutes;
    private Timestamp originalEta;
    private Timestamp latestEta;

    // Constructor
    public TrainDelay(String trainLine, String runNumber, String previousStation,
                      String nextStation, String endStation, long delayInMinutes,
                      Timestamp originalEta, Timestamp latestEta) {
        this.trainLine = trainLine;
        this.runNumber = runNumber;
        this.previousStation = previousStation;
        this.nextStation = nextStation;
        this.endStation = endStation;
        this.delayInMinutes = delayInMinutes;
        this.originalEta = originalEta;
        this.latestEta = latestEta;
    }

    // Getters
    public String getTrainLine() { return trainLine; }
    public String getRunNumber() { return runNumber; }
    public String getPreviousStation() { return previousStation; }
    public String getNextStation() { return nextStation; }
    public String getEndStation() { return endStation; }
    public long getDelayInMinutes() { return delayInMinutes; }
    public Timestamp getOriginalEta() { return originalEta; }
    public Timestamp getLatestEta() { return latestEta; }
}