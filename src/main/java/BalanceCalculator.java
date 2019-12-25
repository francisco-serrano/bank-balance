import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.time.LocalDateTime;
import java.util.Properties;

public class BalanceCalculator {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "balance-calculator");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(LocalDateTime.class, new TransactionMessage.LocalDateTimeSerializer());
        gsonBuilder.registerTypeAdapter(LocalDateTime.class, new TransactionMessage.LocalDateTimeDeserializer());

        Gson gson = gsonBuilder.create();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> transactionsInput = builder.stream("bank.balance.transaction");
        transactionsInput
                .selectKey((k, v) -> {
                    TransactionMessage message = gson.fromJson(v, TransactionMessage.class);
                    return message.customer;
                })
                .groupByKey()
                .reduce((aggValue, newValue) -> {
                    TransactionMessage aggMsg = gson.fromJson(aggValue, TransactionMessage.class);
                    TransactionMessage newMsg = gson.fromJson(newValue, TransactionMessage.class);

                    TransactionMessage message = new TransactionMessage();
                    message.customer = newMsg.customer;
                    message.amount = aggMsg.amount + newMsg.amount;
                    message.time = newMsg.time;

                    return gson.toJson(message);
                })
                .toStream()
                .to("bank.balance.calculation", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
