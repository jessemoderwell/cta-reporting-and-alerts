package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class TrainInfoMapper implements FlatMapFunction<String, TrainInfo> {

    @Override
    public void flatMap(String value, Collector<TrainInfo> out) throws Exception {
        // Parse the incoming JSON
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(value);
        JsonNode trainsNode = jsonNode.path("ctatt").path("route").get(0).path("train");

        // Loop through all trains and collect each one
        for (JsonNode train : trainsNode) {
            String rn = train.path("rn").asText();
            String destSt = train.path("destSt").asText();
            String destNm = train.path("destNm").asText();
            String trDr = train.path("trDr").asText();
            String nextStaId = train.path("nextStaId").asText();
            String nextStpId = train.path("nextStpId").asText();
            String nextStaNm = train.path("nextStaNm").asText();
            String prdt = train.path("prdt").asText();
            String arrT = train.path("arrT").asText();
            String isApp = train.path("isApp").asText();
            String isDly = train.path("isDly").asText();
            String lat = train.path("lat").asText();
            String lon = train.path("lon").asText();
            String heading = train.path("heading").asText();

            // Collect the TrainInfo object
            out.collect(new TrainInfo(rn, destSt, destNm, trDr, nextStaId, nextStpId,
                    nextStaNm, prdt, arrT, isApp, isDly, lat, lon, heading));
        }
    }
}