import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerWithKeyDemo {

  public static void main(String[] args) {

    Logger logger = LoggerFactory.getLogger(KafkaConsumerWithKeyDemo.class);

    String bootstrapServer = "localhost:19092";
    //        String groupId = "gfg-consumer-group";
    String topics = "input-topic";

    // Create Consumer Properties
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // Create Kafka Consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    // Subscribe Consumer to Our Topics
    consumer.subscribe(List.of(topics));
    try {
      //      while (true) {

      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      for (ConsumerRecord<String, String> record : records) {
        logger.info(
            "Key: "
                + record.key()
                + " Value: "
                + record.value()
                + " Partition: "
                + record.partition()
                + " Offset: "
                + record.offset());
      }
      //      }
    } catch (WakeupException e) {

    } finally {
      consumer.close();
    }
    // Poll the data

  }
}
