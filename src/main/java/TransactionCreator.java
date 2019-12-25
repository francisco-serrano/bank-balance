import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class TransactionCreator {
    private List<String> possibleCustomers = Arrays.asList("John", "Mike", "Eddie", "Michael", "Patrick", "Francis");
    private Random random = new Random();

    private String randomCustomer() {
        return this.possibleCustomers.get(this.random.nextInt(this.possibleCustomers.size()));
    }

    private int randomAmount(int max) {
        return this.random.nextInt(max);
    }

    public TransactionMessage generateTransaction() {
        TransactionMessage message = new TransactionMessage();
        message.customer = this.randomCustomer();
        message.amount = this.randomAmount(1000);
        message.time = LocalDateTime.now();

        return message;
    }
}
