FROM streamnative/pulsar-flink:1.15.0.2
COPY target/flink-operator-tests-0.1.0.jar /opt/flink/
