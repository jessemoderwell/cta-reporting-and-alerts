package com.example;

// Import the helper classes
import com.example.StringDeserializationSchema;
import com.example.TrainInfo;
import com.example.TrainInfoMapper;

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
                new StringDeserializationSchema(), // Use the custom deserialization schema
                properties
        );

        // Add the consumer to the execution environment
        DataStream<String> stream = env.addSource(consumer);

        // Process the incoming JSON data
        DataStream<TrainInfo> processedStream = stream.flatMap(new TrainInfoMapper());

        // Write processed results to file
        processedStream.writeAsText("/tmp/test");

        // Execute the job
        env.execute("Kafka Flink Job");
    }
}