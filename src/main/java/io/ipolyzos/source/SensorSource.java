package io.ipolyzos.source;

import io.ipolyzos.models.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Calendar;
import java.util.Random;


public class SensorSource  extends RichParallelSourceFunction<SensorReading> {
    private static final Logger logger = LoggerFactory.getLogger(SensorSource.class);
    private boolean isRunning = true;
    private Random random;

    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("Initializing Sensor Source ...");
        random = new Random();
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        int taskIndex = getRuntimeContext().getIndexOfThisSubtask();

        String[] sensorIds = new String[10];
        double[] curFTemp = new double[10];

        for (int i = 0; i < 10; i++) {
            sensorIds[i] = "Sensor#" + (taskIndex * 10 + i);
            curFTemp[i] = 65 + (random.nextGaussian() * 20);
        }

        while (isRunning) {
            // get current time
            long curTime = Calendar.getInstance().getTimeInMillis();

            // emit SensorReadings
            for (int i = 0; i < 10; i++) {

                curFTemp[i] += random.nextGaussian() * 0.5;

                sourceContext.collect(
                        new SensorReading(sensorIds[i], curTime, curFTemp[i])
                );
            }

            // wait for 100 ms
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }

}
