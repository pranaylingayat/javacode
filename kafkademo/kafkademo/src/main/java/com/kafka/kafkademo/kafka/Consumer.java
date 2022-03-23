package com.kafka.kafkademo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer  {
    public static void main(String[] args) {
        //Creater Logger for the consumer class
        final Logger logger = LoggerFactory.getLogger(Consumer.class);

        //Create variables for strings
        final String bootstrapServers =  "127.0.0.1:9092";
        final String consumerGroupID = "IDC-group-consumer";

        //Create property object for consumer
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupID);
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create Consumer
        final KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(p);

        //Subscribe to the topic
        consumer.subscribe(Arrays.asList("IDC-topic"));

        //Poll and consume records
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); //The Kafka consumer poll() method fetches records in sequential order from a specified topic/partitions. This poll() method is how Kafka clients read data from Kafka. ... After ensuring the consumer is being accessed by a single thread, the loop is entered and messages are consumed.
            for (ConsumerRecord record: records) {
                logger.info("\nReceived record Metadata. \n" + "\nKey: " + record.key() + "," +
                        "Topic: " + record.topic() +", \nPratition: " +record.partition() + "," +
                        "\nOffset: " + record.offset() + "\n@TimeStamp: " + record.timestamp() + "\n");

            }
        }

    }
}
