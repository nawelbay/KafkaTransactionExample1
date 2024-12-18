package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Properties;

public class KafkaTransactionalProducerExample {
    public static void main(String[] args) {


        // Kafka producer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092"); // Connect to Kafka in Docker
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "all");
        properties.put("enable.idempotence", "true");
        properties.put("transactional.id", "order-transactional-id");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        try {
            // Initialize and begin transaction
            producer.initTransactions();
            producer.beginTransaction();

            // Simulate placing an order
            String orderId = "ORDER123";
            String orderDetails = "{\"orderId\": \"" + orderId + "\", \"product\": \"keyboad\", \"quantity\": 1, \"price\": 1200.00}";

            // Create Kafka record
            ProducerRecord<String, String> orderRecord = new ProducerRecord<>("orders", orderId, orderDetails);
            // Send the order
            producer.send(orderRecord);
            // Commit the transaction
            producer.commitTransaction();
            System.out.println("Order sent successfully: " + orderDetails);

        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // Critical error, close producer
            producer.close();
            e.printStackTrace();
        } catch (KafkaException e) {
            // Abort transaction in case of error
            producer.abortTransaction();
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
