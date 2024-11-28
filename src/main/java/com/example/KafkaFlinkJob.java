package com.example;

// Import the helper classes
import com.example.StringDeserializationSchema;
import com.example.TrainInfo;
import com.example.TrainInfoMapper;
import com.example.ETAUpdater;
import com.example.TrainDelayExtractor;

import java.time.LocalDate;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

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

        // Convert TrainInfo to TrainDelay (implement this transformation class)
        DataStream<TrainDelay> delayStream = processedStream
            .flatMap(new TrainDelayExtractor());  // You need to create this class

        // Add the JDBC sink
        delayStream.addSink(
            JdbcSink.sink(
                "INSERT INTO train_delays (train_line, run_number, day, previous_station, next_station, end_station, delay_in_minutes, original_eta, latest_eta) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (statement, delay) -> {
                    statement.setString(1, delay.getTrainLine());
                    statement.setString(2, delay.getRunNumber());
                    statement.setDate(3, java.sql.Date.valueOf(LocalDate.now())); // Adjust if needed
                    statement.setString(4, delay.getPreviousStation());
                    statement.setString(5, delay.getNextStation());
                    statement.setString(6, delay.getEndStation());
                    statement.setLong(7, delay.getDelayInMinutes());
                    statement.setTimestamp(8, delay.getOriginalEta());
                    statement.setTimestamp(9, delay.getLatestEta());
                },
                JdbcExecutionOptions.builder()
                    .withBatchSize(500)
                    .withBatchIntervalMs(200)
                    .withMaxRetries(5)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl("jdbc:mysql://mysql:3306/mydatabase")
                    .withDriverName("com.mysql.cj.jdbc.Driver")
                    .withUsername("myuser")
                    .withPassword("mypassword")
                    .build()
            )
        );


        // Execute the job
        env.execute("Kafka Flink Job");
    }
}