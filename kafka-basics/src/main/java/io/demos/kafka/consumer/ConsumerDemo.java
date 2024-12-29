package io.demos.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger logger = LoggerFactory.getLogger(
            ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        String groupId = "my-java-app";
        String topic = "second_topic";
        logger.info("I am a consumer............. waiting to consume....");


        //create Consumer Properties
        Properties properties = new Properties();
        // connect to LocalHost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        // create the Consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);

        // none - we must set consumer group before starting to consume
        // earliest - read from the beginning
        // latest - read the latest messages,  just from now
        properties.setProperty("auto.offset.reset", "earliest");

        // create a Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe to topic
        consumer.subscribe(List.of(topic));

        // poll for data
        while (true) {
            logger.info("Polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("key: " + record.key() + "| value: " + record.value());
                logger.info("Partition: " + record.partition() + "| offset: " + record.offset());
            }
        }


        // flush and close the consumer
    }
}
