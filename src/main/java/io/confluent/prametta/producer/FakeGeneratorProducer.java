package io.confluent.prametta.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Utils;

import com.github.javafaker.Faker;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

@Getter
@Setter
@Log4j2
public class FakeGeneratorProducer {

    Producer<String, String> producer;
    String topic;
    boolean isRunning;

    @SneakyThrows
    public FakeGeneratorProducer(String configFilepath, String topic) {
        this.producer = new KafkaProducer<>(Utils.loadProps(configFilepath));
        this.topic = topic;
        this.isRunning = true;
    }

    public void produce() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isRunning = false;
        }));

        Faker faker = new Faker();
        while (isRunning) {
            String quote = faker.gameOfThrones().quote();
            String character = faker.gameOfThrones().character();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, character, quote);

            // send data - asynchronous
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    isRunning = false;
                    log.error("Exception: {}", exception.getMessage());
                }
                log.debug("Message sent to partition {} with offset {}", metadata.partition(), metadata.offset());
            });

            // flush data - synchronous
            producer.flush();

        }

        // flush and close producer
        producer.close();
    }
}
