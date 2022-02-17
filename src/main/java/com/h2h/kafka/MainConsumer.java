package com.h2h.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MainConsumer {
    static Logger log = LoggerFactory.getLogger(MainConsumer.class);

    public static void main(String ...args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka2:9092");
        props.put("client.id", "consumer3");
        props.put("group.id", "cdc-consumer2");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("heartbeat.interval.ms", "10000");

        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("cluster-temp", "cluster-temp-sum"), new ConsumerRebalanceListener() {
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
        Map<String, Integer> sensorCount = new HashMap<>();
        Map<String, Integer> sumTemp = new HashMap<>();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            long sumId = 0;
            for (ConsumerRecord<String, String> record : records) {
                if (record.topic().equals("cluster-temp")) {
                    log.info("Log Data {}", record.value());

                    String[] split = record.value().toString().split("\\|");

                    long recordTime = 0;
                    Integer temp = 0;
                    try {
                        recordTime = Long.parseLong(split[1]);

                        temp = Integer.parseInt(split[2]);
                    } catch (NumberFormatException nfe) {
                        log.warn("illegal record {}", record.value());
                    }

                    if (sumId != recordTime % (600*1000)) {
                        sumId = recordTime % (600 * 1000);

                        for (String sensorId : sensorCount.keySet()) {
                            int averageTemp = sumTemp.get(sensorId) / sensorCount.get(sensorId);
                            int sumTime = (int) (recordTime / (600*1000));

                            StringBuilder append = new StringBuilder().append(record.key()).append("|").append(sumTime)
                                    .append("|").append(averageTemp);
                            kafkaProducer(record.key(), append.toString());
                        }
                    } else {
                        Integer count = sensorCount.get(record.key());
                        if (count == null)
                            count = 0;
                        count = count + 1;
                        sensorCount.put(record.key(), count);
                        Integer totalTemp = sumTemp.get(record.key());
                        if (totalTemp == null)
                            totalTemp = 0;
                        totalTemp += temp;

                        sumTemp.put(record.key(), totalTemp);

                    }
                } else if (record.topic().equals("cluster-temp-sum")) {
                    log.info("Record: {}", record.toString());
                }
            }
            if (buffer.size() >= minBatchSize) {
                consumer.commitSync();
                buffer.clear();
            }
        }
    }

    private static void kafkaProducer(String key, String append) {
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

        producer.send(new ProducerRecord<>("cluster-temp-sum", key, append), (metadata, exception) -> {
            if (exception != null)
                log.error("", exception);
        });

        producer.flush();

        producer.close();
    }
}
