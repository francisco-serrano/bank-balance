import com.google.gson.*;

import java.lang.reflect.Type;
import java.time.LocalDateTime;

public class TransactionMessage {
    public static class LocalDateTimeSerializer implements JsonSerializer<LocalDateTime> {
        @Override
        public JsonElement serialize(LocalDateTime localDateTime, Type type, JsonSerializationContext context) {
            return new JsonPrimitive(localDateTime.toString());
        }
    }

    public static class LocalDateTimeDeserializer implements JsonDeserializer<LocalDateTime> {
        @Override
        public LocalDateTime deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context) throws JsonParseException {
            return LocalDateTime.parse(jsonElement.getAsJsonPrimitive().getAsString());
        }
    }

    public String customer;
    public int amount;
    public LocalDateTime time;
}