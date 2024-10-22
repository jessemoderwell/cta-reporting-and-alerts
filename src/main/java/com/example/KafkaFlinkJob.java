package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FlatMapFunction;

import java.io.IOException;
import java.util.Properties;

public class KafkaFlinkJob {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:29092");
        properties.setProperty("group.id", "flink-consumer-group");

        // Create a Kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "train-etas",
                new AbstractDeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        return new String(message);
                    }
                },
                properties
        );

        // Add the consumer to the execution environment
        DataStream<String> stream = env.addSource(consumer);

        // Process the incoming JSON data
        DataStream<TrainInfo> processedStream = stream
            .flatMap(new FlatMapFunction<String, TrainInfo>() {
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
            });

        // Print the processed results to stdout
        processedStream.writeAsText("/tmp/test");

        // Execute the job
        env.execute("Kafka Flink Job");
    }

    // Define a POJO for Train information
    public static class TrainInfo {
        private String rn;
        private String destSt;
        private String destNm;
        private String trDr;
        private String nextStaId;
        private String nextStpId;
        private String nextStaNm;
        private String prdt;
        private String arrT;
        private String isApp;
        private String isDly;
        private String lat;
        private String lon;
        private String heading;

        // Constructor
        public TrainInfo(String rn, String destSt, String destNm, String trDr, String nextStaId,
                         String nextStpId, String nextStaNm, String prdt, String arrT,
                         String isApp, String isDly, String lat, String lon, String heading) {
            this.rn = rn;
            this.destSt = destSt;
            this.destNm = destNm;
            this.trDr = trDr;
            this.nextStaId = nextStaId;
            this.nextStpId = nextStpId;
            this.nextStaNm = nextStaNm;
            this.prdt = prdt;
            this.arrT = arrT;
            this.isApp = isApp;
            this.isDly = isDly;
            this.lat = lat;
            this.lon = lon;
            this.heading = heading;
        }

        // Getters and Setters (optional)

        @Override
        public String toString() {
            return "TrainInfo{" +
                    "rn='" + rn + '\'' +
                    ", destSt='" + destSt + '\'' +
                    ", destNm='" + destNm + '\'' +
                    ", trDr='" + trDr + '\'' +
                    ", nextStaId='" + nextStaId + '\'' +
                    ", nextStpId='" + nextStpId + '\'' +
                    ", nextStaNm='" + nextStaNm + '\'' +
                    ", prdt='" + prdt + '\'' +
                    ", arrT='" + arrT + '\'' +
                    ", isApp='" + isApp + '\'' +
                    ", isDly='" + isDly + '\'' +
                    ", lat='" + lat + '\'' +
                    ", lon='" + lon + '\'' +
                    ", heading='" + heading + '\'' +
                    '}';
        }
    }
}
