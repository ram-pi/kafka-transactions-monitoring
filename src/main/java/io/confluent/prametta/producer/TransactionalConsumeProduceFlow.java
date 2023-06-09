package io.confluent.prametta.producer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Getter
@Setter
public class TransactionalConsumeProduceFlow {

    Producer<String, String> producer;
    Consumer<String, String> consumer;
    String consumeTopic;
    String produceTopic;
    int pollDurationMillis;
    boolean isRunning;

    @SneakyThrows
    public TransactionalConsumeProduceFlow(String configFilepath, String consumeTopic, String produceTopic,
            String transactionalId) {
        Properties props = Utils.loadProps(configFilepath);
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        this.producer = new KafkaProducer<>(props);
        this.consumer = new KafkaConsumer<>(Utils.loadProps(configFilepath));
        this.consumeTopic = consumeTopic;
        this.produceTopic = produceTopic;
        this.isRunning = true;

        pollDurationMillis = 1000;
        if (props.getProperty("poll.ms") != null) {
            pollDurationMillis = Integer.valueOf(props.getProperty("poll.ms"));
        }
    }

    public void consumerAndProduce() {
        Runtime.getRuntime().addShutdownHook(
                new Thread(
                        () -> isRunning = false));

        // Subscribe
        consumer.subscribe(Collections.singleton(consumeTopic));

        // Init transactional producer
        producer.initTransactions();

        while (isRunning) {
            ConsumerRecords<String, String> records = consumer
                    .poll(Duration.ofMillis(pollDurationMillis));
            producer.beginTransaction();
            for (ConsumerRecord<String, String> consumerRecord : records) {
                // Send consumer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(produceTopic, consumerRecord.key(),
                        consumerRecord.value());
                producer.send(producerRecord, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Exception: {}", exception.getMessage());
                        isRunning = false;
                    }
                    log.debug("Message sent to partition {} with offset {}", metadata.partition(), metadata.offset());
                });
            }

            // Transactional offsets
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
                long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
                offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
            }

            // Send offsets
            producer.sendOffsetsToTransaction(offsetsToCommit, consumer.groupMetadata());

            // Commit
            producer.commitTransaction();

            // flush data - synchronous
            producer.flush();
        }

        // consumer close
        consumer.close();

        // flush and close producer
        producer.close();
    }
}
