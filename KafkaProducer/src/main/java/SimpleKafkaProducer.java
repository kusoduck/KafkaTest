
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleKafkaProducer {
	public static void main(String[] args) {
		// 設置 Kafka 生產者的配置
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// 創建 Kafka 生產者
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		// 創建一個 ProducerRecord 對象
		// 主題、分區、KEY、VALUE
		// 分區數量需要在server.properties修改num.partitions
		ProducerRecord<String, String> record = new ProducerRecord<>("my-topic",0, "key", "Hello, Kafka!");
		// 發送消息
		producer.send(record);

		// 主題、分區、KEY、VALUE
		record = new ProducerRecord<>("my-topic", 1, "key", "Hello, Kafka!");
		producer.send(record);

		// 刷新並關閉生產者
		producer.flush();
		producer.close();

		System.out.println("Message sent successfully");
	}
}