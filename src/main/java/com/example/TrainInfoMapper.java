package com.example;

import com.example.ETAUpdater;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import java.util.Set; 
import java.util.HashSet;

public class TrainInfoMapper implements FlatMapFunction<String, TrainInfo> {

    private ETAUpdater etaUpdater;

    public TrainInfoMapper(ETAUpdater etaUpdater) {
        this.etaUpdater = etaUpdater;
    }

    @Override
    public void flatMap(String value, Collector<TrainInfo> out) throws Exception {
        // Parse the incoming JSON
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(value);
        System.out.println(jsonNode);
        JsonNode trainsNode = jsonNode.path("ctatt").path("route").get(0).path("train");

    // Create a set to hold current message keys
    Set<String> currentMessageKeys = new HashSet<>();

    // Loop through all trains and collect each one
    for (JsonNode train : trainsNode) {
        TrainInfo trainInfo = new TrainInfo(
            train.path("rn").asText(),
            train.path("destSt").asText(),
            train.path("destNm").asText(),
            train.path("trDr").asText(),
            train.path("nextStaId").asText(),
            train.path("nextStpId").asText(),
            train.path("nextStaNm").asText(),
            train.path("prdt").asText(),
            train.path("arrT").asText(),
            train.path("isApp").asText(),
            train.path("isDly").asText(),
            train.path("lat").asText(),
            train.path("lon").asText(),
            train.path("heading").asText()
        );

        // Add the key to currentMessageKeys for this train
        currentMessageKeys.add(trainInfo.getRn() + "_" + trainInfo.getNextStaNm() + "_" + trainInfo.getTrDr());
        // System.out.println(currentMessageKeys);
        // Use ETAUpdater to update the ETA map for each train
        etaUpdater.updateETA(trainInfo);
        
        // Emit the updated TrainInfo object downstream
        out.collect(trainInfo);
    }

    // After processing the current message, check etaMap
    etaUpdater.checkAndRecordDelays(currentMessageKeys);
    }
}