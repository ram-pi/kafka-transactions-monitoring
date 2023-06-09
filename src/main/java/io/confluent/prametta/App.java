package io.confluent.prametta;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.utils.Utils;

import io.confluent.prametta.producer.FakeGeneratorProducer;
import io.confluent.prametta.producer.TransactionalConsumeProduceFlow;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class App {
    public static final String CLIENT_CONFIG_PATH = "./client.properties";
    public static final String INPUT_TOPIC = "input";
    public static final String OUTPUT_TOPIC = "output";
    public static final String TRANSACTIONAL_ID = "txn-prod-1";

    @SneakyThrows
    public static void main(String[] args) {
        // Run producers in 2 different threads
        ExecutorService exec = Executors.newCachedThreadPool();

        // 1. Create topics
        log.info("Creating topics...");
        AdminClient adminClient = AdminClient.create(Utils.loadProps(CLIENT_CONFIG_PATH));
        NewTopic newTopic = new NewTopic(INPUT_TOPIC, 10, (short) 3);
        adminClient.createTopics(Collections.singleton(newTopic));
        newTopic = new NewTopic(OUTPUT_TOPIC, 10, (short) 3);
        adminClient.createTopics(Collections.singleton(newTopic));

        // 2. Input Producer
        exec.execute(() -> {
            log.info("Running first producer...");
            FakeGeneratorProducer producer = new FakeGeneratorProducer(CLIENT_CONFIG_PATH, INPUT_TOPIC);
            producer.produce();
        });

        // 3. Transactional Consume-Produce
        exec.execute(() -> {
            log.info("Running transactional consume-produce...");
            TransactionalConsumeProduceFlow transactionalConsumeProduceFlow = new TransactionalConsumeProduceFlow(
                    CLIENT_CONFIG_PATH, INPUT_TOPIC, OUTPUT_TOPIC, TRANSACTIONAL_ID);
            transactionalConsumeProduceFlow.consumerAndProduce();
        });
    }
}
