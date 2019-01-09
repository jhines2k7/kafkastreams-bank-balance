package com.hines.james;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

public class BankBalanceProducer {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(BankBalanceProducer.class);

        String bootstrapServers = "13.56.138.148:9092";

        // create Producer properties
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        String[] customerNames = new String[] {
                "Malene",
                "Hanif",
                "Filomena",
                "Riccardo",
                "Tancred",
                "Noah"
        };

        ObjectMapper objectMapper = new ObjectMapper();

        while(true){
            // create a producer record ~100/sec
            // {"name": "John", "amount": 123, "time": "2019-08-01 01:59:30.274"}
            Random rand = new Random();

            Integer amount = rand.nextInt(200) + 1;

            DepositEvent depositEvent = new DepositEvent(customerNames[rand.nextInt(customerNames.length)], amount);

            ProducerRecord<String, String> record =
                    null;
            try {
                record = new ProducerRecord<>("bank-deposit-events", objectMapper.writeValueAsString(depositEvent));
            } catch (JsonProcessingException e) {
                logger.error("There was an error processing the deposit event", e);
            }

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

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                logger.error("There was an error interrupting the thread", e);
            }
        }

        // flush data
        // producer.flush();
        // flush and close producer
        // producer.close();
    }
}
