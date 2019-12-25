import com.google.gson.*;

import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

public class TransactionBuilder {
    private class LocalDateTimeSerializer implements JsonSerializer<LocalDateTime> {
        @Override
        public JsonElement serialize(LocalDateTime localDateTime, Type type, JsonSerializationContext context) {
            return new JsonPrimitive(localDateTime.toString());
        }
    }

    private class LocalDateTimeDeserializer implements JsonDeserializer<LocalDateTime> {
        @Override
        public LocalDateTime deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context) throws JsonParseException {
            return LocalDateTime.parse(jsonElement.getAsJsonPrimitive().getAsString());
        }
    }

    private class KafkaMessage {
        public String customer;
        public int amount;
        public LocalDateTime time;
    }

    private Gson gson;
    private List<String> possibleCustomers = Arrays.asList("John", "Mike", "Eddie", "Michael", "Patrick", "Francis");


    public TransactionBuilder() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(LocalDateTime.class, new LocalDateTimeSerializer());
        gsonBuilder.registerTypeAdapter(LocalDateTime.class, new LocalDateTimeDeserializer());

        this.gson = gsonBuilder.create();
    }

    public KafkaMessage generateTransaction() {
        KafkaMessage message = new KafkaMessage();
        message.customer = "John";
        message.amount = 123;
        message.time = LocalDateTime.now();

        System.out.println(this.gson.toJson(message));

        return message;
    }
}
