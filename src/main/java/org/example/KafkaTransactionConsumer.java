package org.example;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;

import java.util.Collections;
import java.util.Properties;

public class KafkaTransactionConsumer {
    public static void main(String[] args) {
        // Kafka consumer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092"); // Connect to Kafka in Docker
        properties.put("group.id", "order-processing-group");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("isolation.level", "read_committed"); // Only read committed messages

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the orders topic
        consumer.subscribe(Collections.singletonList("orders"));

        // MongoDB connection
        String mongoUri = "mongodb://localhost:27018"; // Connect to MongoDB in Docker
        try (MongoClient mongoClient = MongoClients.create(mongoUri)) {


            MongoDatabase database = mongoClient.getDatabase("ecommerce");
            MongoCollection<Document> ordersCollection = database.getCollection("orders");
            // Process orders
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    // Parse the order details
                    String orderDetails = record.value();
                    System.out.println("Processing order: " + orderDetails);

                    // Convert JSON-like string to MongoDB document
                    Document orderDocument = Document.parse(orderDetails);
                    orderDocument.append("status", "PROCESSED"); // Add status field

                    // Insert the document into MongoDB
                    ordersCollection.insertOne(orderDocument);
                    System.out.println("Order saved to MongoDB: " + orderDocument.getString("orderId"));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
