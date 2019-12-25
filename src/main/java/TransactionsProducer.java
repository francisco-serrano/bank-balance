import com.google.gson.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class TransactionsProducer {
    private static Logger logger = LoggerFactory.getLogger(TransactionsProducer.class.getName());
    private Gson gson;
    private TransactionCreator creator;

    public TransactionsProducer() {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(LocalDateTime.class, new TransactionMessage.LocalDateTimeSerializer());
        builder.registerTypeAdapter(LocalDateTime.class, new TransactionMessage.LocalDateTimeDeserializer());

        this.gson = builder.create();
        this.creator = new TransactionCreator();
    }

    public void run() {
                /*
            TODO: Parametrize configuration
         */

        List<String> brokers = Collections.singletonList("localhost:9092");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.get(0));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // kafka 2.0 >= 1.1

        // high throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32KB batch size

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("gracefully closing producer...");
            producer.close();
            logger.info("done!");
        }));

        for (int i = 0; i < 1000000; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    "bank.balance.transaction",
                    null,
                    this.gson.toJson(this.creator.generateTransaction())
            );

            producer.send(record, ((recordMetadata, e) -> {
                if (e != null) {
                    logger.error("something bad happened", e);
                }
            }));
        }
    }
}
