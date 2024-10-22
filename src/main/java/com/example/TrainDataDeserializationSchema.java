package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

// Custom DeserializationSchema for TrainData
public class TrainDataDeserializationSchema implements DeserializationSchema<TrainData> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public TrainData deserialize(byte[] message) throws IOException {
        // Deserialize JSON string to TrainData object
        return objectMapper.readValue(message, TrainData.class);
    }

    @Override
    public boolean isEndOfStream(TrainData nextElement) {
        return false; // Change this logic if you want to signal the end of the stream
    }

    @Override
    public TypeInformation<TrainData> getProducedType() {
        return TypeInformation.of(TrainData.class);
    }
}