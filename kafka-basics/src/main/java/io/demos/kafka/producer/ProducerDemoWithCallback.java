package io.demos.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger logger = LoggerFactory.getLogger(
            ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        logger.info("hello world");

        //create Producer Properties
        Properties properties = new Properties();
        // connect to LocalHost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
       // properties.setProperty("batch.size", "400");


        // create the Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);


        for (int j = 0; j <= 10; j++) {

            for (int i = 0; i <= 30; i++) {
                // create a Producer Record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("second_topic", "hello world" +j+", "+i);
                // send data
                kafkaProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // executed a record is successfully sent or an exception occured

                        if (exception == null) {
                            logger.info("Received metadat :\n" +
                                    "Topic : " + metadata.topic() + "\n" +
                                    "Partition : " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp());
                        } else {
                            logger.error("Error while producing :" + exception.getMessage());
                        }
                    }
                });
            }

            Thread.sleep(500);

        }
        ;


        // flush and close the producer

        // tell the producer to send all data and block until done -- synchronous
        kafkaProducer.flush();

        // close the producer
        kafkaProducer.close();
    }
}
