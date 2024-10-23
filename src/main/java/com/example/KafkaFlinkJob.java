package com.example;

// Import the helper classes
import com.example.StringDeserializationSchema;
import com.example.TrainInfo;
import com.example.TrainInfoMapper;
import com.example.ETAUpdater;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

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
                new StringDeserializationSchema(),
                properties
        );

        // Create an instance of ETAUpdater
        ETAUpdater etaUpdater = new ETAUpdater();

        // Process the incoming JSON data using TrainInfoMapper
        DataStream<TrainInfo> processedStream = env.addSource(consumer)
            .flatMap(new TrainInfoMapper(etaUpdater));

        // Write the processed results to a file
        // processedStream.writeAsText("/tmp/test");

        // Execute the job
        env.execute("Kafka Flink Job");
    }
}