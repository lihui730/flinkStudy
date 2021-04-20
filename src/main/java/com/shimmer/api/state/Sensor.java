package com.shimmer.api.state;

/**
 * Created by Hui Li on 2021/4/11 20:12
 */
public class Sensor{
    String name;
    long timestamp;
    double temperature;

    public Sensor(String name, long timestamp, double temperature){
        this.name = name;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }
}