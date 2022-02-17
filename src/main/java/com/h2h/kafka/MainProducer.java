package com.h2h.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class MainProducer {
    static Logger log = LoggerFactory.getLogger(MainProducer.class);

    public static void main(String[] args) {
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

        Random random = new Random();
        for(int i = 0; i < 10000; i++) {
            producer.send(new ProducerRecord<>("innova-cluster-test", Integer.toString(random.nextInt()),
                    Integer.toString(random.nextInt())), (metadata, exception) -> {
                if (exception != null)
                    log.error("", exception);
            });
        }

        producer.flush();

        Map<MetricName, ? extends Metric> metrics = producer.metrics();

        log.info("metrics {}", metrics.toString());

        producer.close();
    }
}
