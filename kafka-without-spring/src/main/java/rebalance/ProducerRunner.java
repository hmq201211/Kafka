package rebalance;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProducerRunner {
    private static final int SIZE = 50;
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private static final CountDownLatch COUNT_DOWN_LATCH = new CountDownLatch(SIZE);

    private static class Worker implements Runnable {
        private final KafkaProducer<String, String> producer;
        private final ProducerRecord<String, String> record;

        public Worker(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
            this.producer = producer;
            this.record = record;
        }

        @Override
        public void run() {
            final String id = Thread.currentThread().getId() + "-" + System.identityHashCode(producer);
            producer.send(record, (data, error) -> {
                if (data != null) {
                    System.out.println(id + "|" + String.format("偏移量: %s, 分区: %s", data.offset(), data.partition()));
                }
                if (error != null)
                    error.printStackTrace();
            });
            System.out.println(id + ":数据[" + record + "]已发送.");
            COUNT_DOWN_LATCH.countDown();
        }
    }

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties)) {
            ProducerRecord<String, String> record;
            for (int i = 0; i < SIZE; i++) {
                record = new ProducerRecord<>("rebalance", String.valueOf(i), String.valueOf(i) + "_ReBalance");
                EXECUTOR_SERVICE.execute(new Worker(producer, record));
            }
            COUNT_DOWN_LATCH.await();
        } catch (InterruptedException exception) {
            exception.printStackTrace();
        } finally {
            EXECUTOR_SERVICE.shutdown();
        }
    }
}