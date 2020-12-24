package rebalance;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class ConsumerWorker implements Runnable {
    private final boolean flag;
    private final KafkaConsumer<String, String> consumer;
    private final HashMap<TopicPartition, OffsetAndMetadata> currentOffsets;

    public ConsumerWorker(boolean flag) {
        this.flag = flag;
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, ConsumerRunner.GROUP_ID);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        this.consumer = new KafkaConsumer<>(properties);
        this.currentOffsets = new HashMap<>();
        this.consumer.subscribe(Collections.singletonList("reBalance"), new HandlerReBalance(currentOffsets, consumer));
    }

    @Override
    public void run() {
        final String id = Thread.currentThread().getId() + "";
        int count = 0;
        TopicPartition topicPartition;
        long offset;
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(id + "|" + String.format("处理主题: %s, 分区: %d, 偏移量: %d, key: %s, value: %s ", record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                    topicPartition = new TopicPartition(record.topic(), record.partition());
                    offset = record.offset() + 1;
                    currentOffsets.put(topicPartition, new OffsetAndMetadata(offset, "no-metadata"));
                    count++;
                }
                if (currentOffsets.size() > 0) {
                    for (TopicPartition partition : currentOffsets.keySet()) {
                        HandlerReBalance.partitionOffsetMap.put(partition, currentOffsets.get(partition).offset());
                    }
                }
                if (flag && count >= 5) {
                    System.out.println(id + "-即将关闭, 当前偏移量为: " + currentOffsets);
                    consumer.commitSync();
                    break;
                }
                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }
    }
}
