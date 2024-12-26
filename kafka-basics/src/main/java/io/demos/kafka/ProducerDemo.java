package io.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger logger = LoggerFactory.getLogger(
            ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("hello world");

        //create Producer Properties
        Properties properties = new Properties();
        // connect to LocalHost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // create a Producer Record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("second_topic", "hello world");
        // send data
        kafkaProducer.send(producerRecord);

        // flush and close the producer

        // tell the producer to send all data and block until done -- synchronous
        kafkaProducer.flush();

        // close the producer
        kafkaProducer.close();
    }
}
