
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaTestProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaTestProducer.class);

    private final KafkaProducer<String, String> producer;

    public KafkaTestProducer(String bootstrapServers) {
        Properties props = new Properties();

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }

    public void send(String topicName, String key, String movie) {
        final ProducerRecord<String,String> record = new ProducerRecord<>(topicName, key, movie);
        producer.send(record, (metadata, e) -> {
            if (e != null)
                log.info("Send failed for record {}", record, e);
            else
                log.info("Sent key={}, Partition-{} - Offset {}", record.key(),
                        metadata.partition(), metadata.offset());
        });
    }
}
