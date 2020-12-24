package rebalance;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HandlerReBalance implements ConsumerRebalanceListener {
    public static ConcurrentHashMap<TopicPartition, Long> partitionOffsetMap = new ConcurrentHashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets;
    private final KafkaConsumer<String, String> consumer;

    public HandlerReBalance(HashMap<TopicPartition, OffsetAndMetadata> currentOffsets, KafkaConsumer<String, String> consumer) {
        this.currentOffsets = currentOffsets;
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        final String id = Thread.currentThread().getId() + "";
        System.out.println(id + "-onPartitionsRevoked参数为:" + collection);
        System.out.println(id + "-服务器准备分区再均衡, 提交偏移量. 当前偏移量为" + currentOffsets);
        System.out.println("分区偏移量表" + partitionOffsetMap);
        for (TopicPartition topicPartition : collection) {
            partitionOffsetMap.put(topicPartition, currentOffsets.get(topicPartition).offset());
        }
        consumer.commitSync(currentOffsets);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        final String id = Thread.currentThread().getId() + "";
        System.out.println(id + "-再均衡完成, onPartitionsAssigned参数值为: " + collection);
        System.out.println("分区偏移量表中: " + partitionOffsetMap);
        for (TopicPartition topicPartition : collection) {
            System.out.println(id + "-topicPartition" + topicPartition);
            Long offset = partitionOffsetMap.get(topicPartition);
            if (offset == null) continue;
            consumer.seek(topicPartition, partitionOffsetMap.get(topicPartition));
        }
    }
}
