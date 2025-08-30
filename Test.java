import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

// @Slf4j
public class Test {
  public static void main(String[] args) throws Exception {
    String key =
        "message:[10, 7, 69, 110, 103, 108, 105, 115, 104, 18, 12, 72, 101, 108, 108, 111, 44, 32, 82, 97, 106, 97, 33, 24, 101]__Headers: {job-name=flink-job, type=com.workforcesoftware.grpc.helloworld.v1.HelloWorldOuterClass$HelloWorld}";
    Headers headers = new RecordHeaders();
    // Split the string into message and headers parts
    String[] parts = key.split("__");

    // Extract message
    String message = parts[0].substring(parts[0].indexOf("["));
    System.out.println("Message: " + message);

    // Extract and parse headers
    String headersStr = parts[1].substring(parts[1].indexOf("{") + 1, parts[1].length() - 1);
    Map<String, String> headersMap =
        Arrays.stream(headersStr.split(","))
            .map(String::trim)
            .map(header -> header.split("="))
            .collect(Collectors.toMap(header -> header[0], header -> header[1]));

    headersMap.forEach((k, v) -> headers.add(k, v.getBytes()));

    // To verify the headers were added, you can iterate through them
    for (Header header : headers) {
      System.out.println(header.key() + ": " + new String(header.value()));
    }
  }
}
