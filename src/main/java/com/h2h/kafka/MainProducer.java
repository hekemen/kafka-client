package com.h2h.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MainProducer {
    static Logger log = LoggerFactory.getLogger(MainProducer.class);
    static final int MAX_TEMP = 50;
    static final int MIN_TEMP = -50;

    public int generateTemperature() {
        Random random = new Random();
        return random.ints(MIN_TEMP, MAX_TEMP)
                .findFirst()
                .getAsInt();
    }

    public List<String> sensors() {
        return Arrays.asList("sensor-1", "sensor-2", "sensor-3");
    }

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka1:9092");
        props.put("client.id", "cdc-producer");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 512);
        props.put("linger.ms", 100000);
        props.put("request.timeout.ms", 100000);

        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        Producer<String, String> producer = new KafkaProducer<>(props);
        MainProducer mainProducer = new MainProducer();
        List<String> sensors = mainProducer.sensors();

        Random random = new Random();

        for(int i = 0; i < 10000; i++) {
            int sensorIndex = random.nextInt(3);
            String sensorStr = sensors.get(sensorIndex);
            int temp = mainProducer.generateTemperature();
            long currentMs = System.currentTimeMillis();
            String payload = new StringBuilder().append(sensorStr).append("|").append(currentMs).append("|").append(i).toString();

            producer.send(new ProducerRecord<>("cluster-temp", sensorStr, payload), (metadata, exception) -> {
                if (exception != null)
                    log.error("", exception);
            });
        }

        producer.flush();

        Map<MetricName, ? extends Metric> metrics = producer.metrics();

        for (MetricName name : metrics.keySet()) {
            log.info("metrics {} {}", name, metrics.get(name).metricValue());
        }

        producer.close();
    }
}
