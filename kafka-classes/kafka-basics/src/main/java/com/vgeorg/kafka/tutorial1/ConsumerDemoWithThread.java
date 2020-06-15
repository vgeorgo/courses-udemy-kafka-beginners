package com.vgeorg.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread
{
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private CountDownLatch latch;
    private Logger logger;

    private ConsumerDemoWithThread() {
        this.logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
    }

    private void run() {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer thread");
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application got interrupted", e);
            e.printStackTrace();
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private Logger logger;
        private KafkaConsumer<String, String> consumer;
        private Properties properties;
        private String topic;

        public ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;
            this.logger = LoggerFactory.getLogger(ConsumerRunnable.class);
            this.topic = topic;
            this.setProperties(bootstrapServers, groupId);
        }

        protected void setProperties(String bootstrapServers, String groupId) {
            this.properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }

        @Override
        public void run() {
            this.consumer = new KafkaConsumer<String, String>(this.properties);
            consumer.subscribe(Collections.singleton(this.topic));

            try {
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            }
            catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            }
            finally {
                consumer.close();
                // Tell main that this thread is done
                latch.countDown();
            }

        }

        public void shutdown() {
            // Interrupts .poll() throwing WakeUpException
            consumer.wakeup();
        }
    }
}
