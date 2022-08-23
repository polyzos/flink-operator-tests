package io.ipolyzos.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {
    private String sensorId;
    private long timestamp;
    private double temperature;
}
