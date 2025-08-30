import com.google.protobuf.Message;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerWithKeyDemo {
  public static void main(String[] args)
      throws ExecutionException,
          InterruptedException,
          ClassNotFoundException,
          NoSuchMethodException,
          InvocationTargetException,
          IllegalAccessException {

    Logger logger = LoggerFactory.getLogger(KafkaProducerWithKeyDemo.class);

    String bootstrapServer = "127.0.0.1:19092";

    // Create Producer Properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Create the Producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    for (int i = 0; i < 10; i++) {

      String topic = "input-topic";
      String value = "{ name:Raja, message: Helloworld, id:1 }";
      String key = "id_" + i;

      // Log the Key
      logger.info("Key: " + key);

      // Create a Producer Record with Key
      String className = "com.workforcesoftware.grpc.helloworld.v1.HelloWorldOuterClass$HelloWorld";

      Class<?> clazz = Class.forName(className);

      Method newBuilderMethod = clazz.getMethod("newBuilder");

      Method[] methods = clazz.getDeclaredMethods();
      Class<?>[] innerClasses = clazz.getDeclaredClasses();

      Object builder = newBuilderMethod.invoke(null); // static method, so null for instance

      // Get the setMessage(String) method on the builder

      for (Method method : builder.getClass().getDeclaredMethods()) {
        if (method.getName().startsWith("set")
            && !method.getName().endsWith("Bytes")
            && method.getParameterCount() == 1) {
          logger.info("=====================");
          logger.info(String.valueOf(method.getName()));
          logger.info(String.valueOf(method.getParameterTypes()));
          for (Class<?> pa : method.getParameterTypes()) {
            System.out.println(pa.getName());
          }
        }
      }

      Method setMessageMethod = builder.getClass().getMethod("setMessage", String.class);

      Method setObjectId = builder.getClass().getMethod("setObjectId", long.class);

      Method setId = builder.getClass().getMethod("setId", String.class);

      setId.invoke(builder, "English");
      setObjectId.invoke(builder, 101L);
      setMessageMethod.invoke(builder, "Hello, Raja!");

      Method buildMethod = builder.getClass().getMethod("build");
      Message protomessage = (Message) buildMethod.invoke(builder);
      byte[] binaryData = protomessage.toByteArray();
      Method parseFrom = clazz.getMethod("parseFrom", byte[].class);
      Message mes = (Message) parseFrom.invoke(null, binaryData);
      logger.info(String.valueOf(mes));
      logger.info(Arrays.toString(binaryData));
      logger.info("this is deserialized proto");

      String val =
          "Value:id: \"English\"\n"
              + "message: \"Hello, Raja!\"\n"
              + "object_id: 101\n"
              + "Headers: RecordHeaders(headers = [RecordHeader(key = type, value = [99, 111, 109, 46, 119, 111, 114, 107, 102, 111, 114, 99, 101, 115, 111, 102, 116, 119, 97, 114, 101, 46, 103, 114, 112, 99, 46, 104, 101, 108, 108, 111, 119, 111, 114, 108, 100, 46, 118, 49, 46, 72, 101, 108, 108, 111, 87, 111, 114, 108, 100, 79, 117, 116, 101, 114, 67, 108, 97, 115, 115, 36, 72, 101, 108, 108, 111, 87, 111, 114, 108, 100]), RecordHeader(key = job-name, value = [102, 108, 105, 110, 107, 45, 106, 111, 98])], isReadOnly = false)";
      System.out.println(val);

      ProducerRecord<String, String> record =
          new ProducerRecord<>(topic, key, binaryData.toString());

      record
          .headers()
          .add(
              "type",
              "com.workforcesoftware.grpc.helloworld.v1.HelloWorldOuterClass$HelloWorld"
                  .getBytes());
      record.headers().add("job-name", "flink-job".getBytes());

      // Java Producer with Callback
      producer
          .send(
              record,
              (recordMetadata, e) -> {
                // Executes every time a record successfully sent
                // or an exception is thrown
                if (e == null) {
                  logger.info(
                      "Received new metadata. \n"
                          + "Topic: "
                          + recordMetadata.topic()
                          + "\n"
                          + "Partition: "
                          + recordMetadata.partition()
                          + "\n"
                          + "Offset: "
                          + recordMetadata.partition()
                          + "\n");
                } else {
                  logger.error("Error while producing ", e);
                }
              })
          .get(); // Block the .send() to make it synchronous
    }

    // Flush and Close the Producer
    producer.flush();
    producer.close();
  }
}
