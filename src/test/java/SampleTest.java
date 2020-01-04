import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class SampleTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;
    private Gson gson;

    @Before
    public void setupTopologyTestDriver() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        BalanceCalculator calculator = new BalanceCalculator();

        StringSerializer stringSerializer = new StringSerializer();
        StringDeserializer stringDeserializer = new StringDeserializer();

        this.testDriver = new TopologyTestDriver(calculator.createTopology(), config);

        this.inputTopic = this.testDriver.createInputTopic(
                "bank.balance.transaction", stringSerializer, stringSerializer
        );

        this.outputTopic = this.testDriver.createOutputTopic(
                "bank.balance.calculation", stringDeserializer, stringDeserializer
        );

        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(LocalDateTime.class, new TransactionMessage.LocalDateTimeSerializer());
        builder.registerTypeAdapter(LocalDateTime.class, new TransactionMessage.LocalDateTimeDeserializer());

        this.gson = builder.create();
    }

    @After
    public void closeTestDriver() {
        this.testDriver.close();
    }

    @Test
    public void dummyTest() {
        String dummy = "Du" + "mmy";
        assertEquals(dummy, "Dummy");
    }

    @Test
    public void sampleTest() {
        TransactionMessage message = new TransactionMessage();
        message.customer = "John";
        message.amount = 500;
        message.time = LocalDateTime.now();

        String tx = this.gson.toJson(message);

        pushNewInputRecord(message.customer, tx);

        TestRecord<String, String> output = readOutput();

        assertEquals(tx, output.value());
    }

    public void pushNewInputRecord(String key, String value) {
        this.inputTopic.pipeInput(key, value);
    }

    public TestRecord<String, String> readOutput() {
        return this.outputTopic.readRecord();
    }
}
