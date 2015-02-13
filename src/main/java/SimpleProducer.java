import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author Abhilash S T P
 */
public class SimpleProducer {
    private static Producer<Integer, String> producer;
    private final Properties properties = new Properties();

    public SimpleProducer() {
        properties.put("bootstrap.servers", "<broker_ip>:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer, String>(properties);
    }

    public static void main(String[] args) {
        SimpleProducer simpleProducer = new SimpleProducer();
        String topic = args[0];
        String messageStr = args[1];
        ProducerRecord<Integer, String> data = new ProducerRecord<Integer, String>(topic, messageStr);
        producer.send(data);
        producer.close();
    }
}
