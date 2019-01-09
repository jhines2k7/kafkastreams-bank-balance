package com.hines.james;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class BankBalanceProducer {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(BankBalanceProducer.class);

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        for (int i = 0; i < 10; i++ ) {
            // create a producer record
            // {"name": "John", "amount": 123, "time": "2018-07-19T05:24:52"}
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("first_topic", "hello world " + Integer.toString(i));

            // send data - asynchronous
            producer.send(record, (RecordMetadata recordMetadata, Exception e) -> {
                // executes every time a record is successfully sent or an exception is thrown
                if (e == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata. \n" +
                            "Topic:" + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            });
        }

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
