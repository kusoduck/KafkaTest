import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SimpleKafkaConsumer {
    public static void main(String[] args) {
        // 設置 Kafka 消費者的配置
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 創建 Kafka 消費者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 手動指定要處理的分區，例如 test 的分區 0
//        TopicPartition partition0 = new TopicPartition("my-topic", 0);
//        TopicPartition partition1 = new TopicPartition("my-topic", 1);
//        consumer.assign(Arrays.asList(partition0, partition1));


        // 訂閱主題
        consumer.subscribe(Arrays.asList("my-topic"));

        // 持續輪詢新消息
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received message: key = %s, value = %s, partition = %d, offset = %d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }

            }
        } finally {
            consumer.close();
        }
    }
}
