package io.ipolyzos.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WindowedReadingAverage {
    private Timestamp windowStart;
    private Timestamp windowEnd;
    private String sensorId;
    private long averageTemperature;
}
