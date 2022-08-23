package io.ipolyzos;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.SubscriptionType;

public class TestPulsarSource {
    public static void main(String[] args) throws Exception {
        // 1. Create an execution environment
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Create a Pulsar Source
        PulsarSource<String> customerSource =
                PulsarSource
                        .builder()
                        .setServiceUrl("pulsar://sn-sn-platform-proxy-headless.sn:6650")
                        .setAdminUrl("http://sn-sn-platform-proxy-headless.sn:8080")
                        .setStartCursor(StartCursor.earliest())
                        .setTopics("test-topic")
                        .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
                        .setSubscriptionName("test-subs")
                        .setUnboundedStopCursor(StopCursor.never())
                        .setSubscriptionType(SubscriptionType.Exclusive)
                        .setConfig(PulsarOptions.PULSAR_AUTH_PLUGIN_CLASS_NAME, "org.apache.pulsar.client.impl.auth.AuthenticationToken")
                        .setConfig(PulsarOptions.PULSAR_AUTH_PARAMS, "token:")
                        .build();

        // 4. Create a DataStream
        DataStream<String> stream =
                environment
                        .fromSource(customerSource, WatermarkStrategy.noWatermarks(), "Test Source")
                        .name("TestSource")
                        .uid("TestSource");

        // 5. Print it to the console
        stream
                .print()
                .uid("print")
                .name("print");

        // 6. Execute the program
        environment.execute("Test Pulsar Source Stream");
    }
}
