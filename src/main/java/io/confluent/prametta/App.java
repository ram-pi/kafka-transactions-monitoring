package io.confluent.prametta;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
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
    public static final String TRANSACTIONAL_ID = "txn-prod";
    public static final int CONCURRENT_EXEC = 100;

    @SneakyThrows
    public static void main(String[] args) {
        Properties props = Utils.loadProps(CLIENT_CONFIG_PATH);
        int concurrent = CONCURRENT_EXEC;
        if (props.getProperty("concurrent") != null) {
            concurrent = Integer.valueOf(props.getProperty("concurrent"));
        }

        // Run producers in 2 different threads
        ExecutorService exec = Executors.newCachedThreadPool();

        // Create topics
        Map<String, String> topicConfig = new HashMap<>();
        topicConfig.put("delete.retention.ms", "60000");
        topicConfig.put("min.insync.replicas", "2");
        log.info("Creating topics...");
        AdminClient adminClient = AdminClient.create(Utils.loadProps(CLIENT_CONFIG_PATH));
        NewTopic newTopic = new NewTopic(INPUT_TOPIC, 10, (short) 3);
        newTopic.configs(topicConfig);
        adminClient.createTopics(Collections.singleton(newTopic));
        newTopic = new NewTopic(OUTPUT_TOPIC, 10, (short) 3);
        newTopic.configs(topicConfig);
        adminClient.createTopics(Collections.singleton(newTopic));

        // Source Producer
        for (int i = 0; i < concurrent; i++) {
            exec.execute(() -> {
                log.info("Running source producer...");
                FakeGeneratorProducer producer = new FakeGeneratorProducer(CLIENT_CONFIG_PATH, INPUT_TOPIC);
                producer.produce();
            });
        }

        // Transactional Consume-Produce Flow
        for (int i = 0; i < concurrent; i++) {
            exec.execute(() -> {
                log.info("Running transactional consume-produce flow...");
                TransactionalConsumeProduceFlow transactionalConsumeProduceFlow = new TransactionalConsumeProduceFlow(
                        CLIENT_CONFIG_PATH, INPUT_TOPIC, OUTPUT_TOPIC,
                        TRANSACTIONAL_ID.concat("-").concat(UUID.randomUUID().toString()));
                transactionalConsumeProduceFlow.consumerAndProduce();
            });
        }
    }
}
