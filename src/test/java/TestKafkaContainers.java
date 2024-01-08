
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestKafkaContainers {
    private static final Logger log = LoggerFactory.getLogger(TestKafkaContainers.class);
    private final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"));

    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void startContainer() {
        kafka.start();
    }

    @Test
    void basicConsumeProduceConsumeTest() throws JsonProcessingException {
        KafkaTestConsumer kafkaTestConsumer = new KafkaTestConsumer(kafka.getBootstrapServers(), "test_group");

        kafkaTestConsumer.subscribe(singletonList("catalog_topic"));

        String movieId = UUID.randomUUID().toString();

        ConsumerRecords<String, String> records = kafkaTestConsumer.poll();

        assertEquals(0,records.count());

        KafkaTestProducer kafkaTestProducer = new KafkaTestProducer(kafka.getBootstrapServers());

        kafkaTestProducer.send("catalog_topic", movieId, movieId);

        records = kafkaTestConsumer.poll();

        assertEquals(1,records.count());

        // As mentioned before, in this example we are using String serializers, in practice it is very common to use Avro schemas to serialize these messages
        records.iterator().forEachRemaining(record -> {
            assertEquals(movieId,record.value());
        });
    }
}
