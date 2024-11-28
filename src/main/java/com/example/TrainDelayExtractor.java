package com.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.sql.Timestamp;

import java.util.Map;
import java.util.Set; 
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class TrainDelayExtractor implements FlatMapFunction<TrainInfo, TrainDelay> {

    public TrainDelayExtractor() {
        // Constructor can be empty or initialize other fields if needed
    }

    @Override
    public void flatMap(TrainInfo trainInfo, Collector<TrainDelay> out) {
        try {
            Map<String, Map<String, Instant[]>> etaMap = ETAUpdater.getEtaMap();
            if (etaMap == null) {
                System.err.println("ETA map is null");
                return;
            }
            
            // System.out.println("etaMap in TrainDelayExtractor:");
            // System.out.println(etaMap);

            String trainLine = ETAUpdater.deriveTrainLine(trainInfo.getRn());
            if (trainLine == null) {
                System.err.println("Could not derive train line for RN: " + trainInfo.getRn());
                return;
            }

            Map<String, Instant[]> currTrainLineEtas = etaMap.get(trainLine);
            if (currTrainLineEtas == null) {
                System.err.println("No ETAs found for train line: " + trainLine);
                return;
            }

            String etaKey = trainInfo.getRn() + "_" + trainInfo.getNextStaNm() + "_" + trainInfo.getTrDr();
            Instant[] etaTimes = currTrainLineEtas.getOrDefault(etaKey, new Instant[2]);
            if (etaTimes[0] == null || etaTimes[1] == null) {
                System.err.println("Invalid ETA times for key: " + etaKey);
                return;
            }

            long delayInMinutes = ChronoUnit.MINUTES.between(etaTimes[0], etaTimes[1]);

            Integer trainDirection = ETAUpdater.getDirection(trainInfo.getTrDr());
            CtaTrainLines CtaTrainLine = new CtaTrainLines();
            String[] stationOrder = CtaTrainLine.getStationOrder(ETAUpdater.deriveTrainLine(trainInfo.getRn()));
            String nextStation = trainInfo.getNextStaNm();
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

            TrainDelay delay = new TrainDelay(
                trainLine,
                trainInfo.getRn(),
                prevStation,
                trainInfo.getNextStaNm(),
                endStation,
                delayInMinutes,
                Timestamp.from(etaTimes[0]),
                Timestamp.from(etaTimes[1])
            );
            out.collect(delay);
        } catch (Exception e) {
            System.err.println("Error processing TrainInfo: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
