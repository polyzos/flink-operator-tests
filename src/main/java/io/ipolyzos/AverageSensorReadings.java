package io.ipolyzos;

import io.ipolyzos.models.SensorReading;
import io.ipolyzos.models.WindowedReadingAverage;
import io.ipolyzos.source.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

public class AverageSensorReadings {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.getConfig().setAutoWatermarkInterval(1000L);
        executionEnvironment.setParallelism(1);

        WatermarkStrategy<SensorReading> watermarkStrategy =
                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<SensorReading>) (sr, l) -> sr.getTimestamp()
                        );

        DataStream<SensorReading> sensorSource = executionEnvironment
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("SensorSource")
                .uid("SensorSource");

        DataStream<WindowedReadingAverage> windowedTempAverage = sensorSource
                .map(r -> new SensorReading(r.getSensorId(), r.getTimestamp(), (r.getTemperature() - 32) * (5.0 / 9.0)))
                .keyBy(SensorReading::getSensorId)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply(new AverageTemperatureFunc());

        windowedTempAverage.print();
        executionEnvironment.execute("Compute Average Sensor Temperature.");
    }

    public static class AverageTemperatureFunc implements WindowFunction<SensorReading, WindowedReadingAverage, String, TimeWindow> {


        @Override
        public void apply(String key, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<WindowedReadingAverage> collector) throws Exception {
            // compute the average temperature
            long sum = StreamSupport.stream(iterable.spliterator(), true)
                    .map(SensorReading::getTemperature)
                    .mapToLong(Double::longValue).sum();

            int count = StreamSupport.stream(iterable.spliterator(), true).collect(Collectors.toList()).size();

            Timestamp windowStart = new Timestamp(timeWindow.getStart());
            Timestamp windowEnd = new Timestamp(timeWindow.getEnd());
            collector
                    .collect(
                            new WindowedReadingAverage(windowStart, windowEnd, key, sum / count
                    ));
        }
    }
}
