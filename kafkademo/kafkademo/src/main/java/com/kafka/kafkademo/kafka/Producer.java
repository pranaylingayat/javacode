package com.kafka.kafkademo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(Producer.class);

        //Create property objects for Producer
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1.:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //bootstrap. servers is a comma-separated list of host and port pairs that are the addresses of the Kafka brokers in a "bootstrap" Kafka cluster that a Kafka client connects to initially to bootstrap itself. Kafka broker. A Kafka cluster is made up of multiple Kafka Brokers. Each Kafka Broker has a unique ID (number)
        //Key Serialization is the process of converting objects into bytes. Deserialization is the inverse process — converting a stream of bytes into an object. In a nutshell, it transforms the content into readable and interpretable information.

        //Create the Producer
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        for (int i=51; i<71; i++) {
            //Create the ProducerRecord
            ProducerRecord<String, String> record = new ProducerRecord<>("IDC-topic", "key_" +i, "IDC_" +i);

            //Send Data Asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        logger.info("\nReceived record Metadata. \n" +
                                "Topic: " + recordMetadata.topic() +", \nPratition: " +recordMetadata.partition() + "," +
                                "\nOffset: " + recordMetadata.offset() + "\n@TimeStamp: " + recordMetadata.timestamp() + "\n");

                    } else {
                        logger.error("Error Occured", e);
                    }
                }
            });
        }



        //flush and close Procedure
        producer.flush(); //The flush() call gives a convenient way to ensure all previously sent messages have actually completed. This example shows how to consume from one Kafka topic and produce to another Kafka topic: for(ConsumerRecord<String, String> record: consumer. poll(100)) producer.
        producer.close(); //A Kafka Producer has a pool of buffer that holds to-be-sent records. The producer has background, I/O threads for turning records into request bytes and transmitting requests to Kafka cluster. The producer must be closed to not leak resources, i.e., connections, thread pools, buffers.
    }

}