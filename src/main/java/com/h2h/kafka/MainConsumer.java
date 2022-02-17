package com.h2h.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MainConsumer {
    static Logger log = LoggerFactory.getLogger(MainConsumer.class);

    public static void main(String ...args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka2:9092");
        props.put("client.id", "consumer1");
        props.put("group.id", "cdc-consumer");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("heartbeat.interval.ms", "10000");

        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("innova-cluster-test"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("Partition Revoked {}", partitions.toString());
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("Partition Assigned {}", partitions.toString());
            }
        });

        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
                log.info("Record: {}", record.value());
            }
            if (buffer.size() >= minBatchSize) {
                for (ConsumerRecord<String, String> r : buffer)
                    log.info("Record: {}", r.toString());
                consumer.commitSync();
                buffer.clear();
            }
        }

    }
}
