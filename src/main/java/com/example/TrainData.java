package com.example;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true) // Ignore unknown properties
public class TrainData {
    @JsonProperty("train") // Assuming the JSON has a "train" property
    private String train;

    @JsonProperty("name") // Assuming the JSON has a "name" property
    private String name;

    @JsonProperty("ctatt") // If you want to include this property
    private String ctatt;

    // Getters and Setters

    public String getTrain() {
        return train;
    }

    public void setTrain(String train) {
        this.train = train;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCtatt() {
        return ctatt;
    }

    public void setCtatt(String ctatt) {
        this.ctatt = ctatt;
    }
}