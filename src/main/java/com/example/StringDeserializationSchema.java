package com.example;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class StringDeserializationSchema extends AbstractDeserializationSchema<String> {
    @Override
    public String deserialize(byte[] message) throws IOException {
        return new String(message);
    }
}