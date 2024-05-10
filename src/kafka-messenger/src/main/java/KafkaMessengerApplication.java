import com.kafka.messengerapp.KafkaMessengerConsumer;
import com.kafka.messengerapp.KafkaMessengerProducer;
import com.kafka.messengerapp.model.Message;
import reactor.core.publisher.Flux;

import java.util.Objects;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaMessengerApplication {
    private static final Scanner SCANNER = new Scanner(System.in);
    private static final String TOPIC = "kafka-messenger-censored-texts";
    private static final String SENDER_CMD = "sender";
    private static final String RECEIVER_CMD = "receiver";

    public static void main(String[] args) {
        String userId;
        String userType;
        System.out.print("enter user type [sender/receiver] > ");
        String command = SCANNER.nextLine();
        if (!isValidCommand(command))
            return;
        userType = command;
        System.out.print("enter user ID (a unique string) > ");
        userId = SCANNER.nextLine();

        if (userType.equals(SENDER_CMD)) {
            System.out.print("enter messages in the format '(<userid>) <message>'\n");
            KafkaMessengerProducer p = new KafkaMessengerProducer();
            Flux.<Message>generate(sink -> {
                        sink.next(Objects.requireNonNull(Command.build(userType, userId)));
                    })
                    .flatMap(p::send)
                    .blockLast();
            p.close();
        } else {
            KafkaMessengerConsumer c = new KafkaMessengerConsumer(userId);

            c.consume(TOPIC).subscribe(rec -> {
                String[] parsedMsg = splitIntoTwoParts(rec.value());
                System.out.printf("(%s) %s\n", parsedMsg[0], parsedMsg[1]);
            });
        }
    }

    private static String[] splitIntoTwoParts(String input) {
        String[] parts = input.split("\\s+", 2);
        if (parts.length == 2) {
            return parts;
        } else {
            return new String[]{parts[0], ""};
        }
    }

    private static boolean isValidCommand(String cmd) {
        return cmd.equals(SENDER_CMD) || cmd.equals(RECEIVER_CMD);
    }

    private static abstract class Command {
        private static Message build(String userType, String userId) {

            if (userType.equals(SENDER_CMD)) {
                return new SenderCommand(userId).build();
            }
            return null;
        }

    }

    private static class SenderCommand extends Command {
        private final String fromUserId;
        public SenderCommand (String fromUserId) {
            this.fromUserId = fromUserId;
        }
        public Message build() {
            String messageTxt;
            do {
                System.out.print("> ");
                messageTxt = SCANNER.nextLine();
                if (badInputFormat(messageTxt)) {
                    System.out.print("Message is in wrong format, try again with the right format!\n");
                }
            } while (messageTxt == null || messageTxt.isEmpty() || badInputFormat(messageTxt));

            String[] parsedMsg = splitMessage(messageTxt);
            if (parsedMsg != null) {
                return new Message(this.fromUserId, parsedMsg[0], parsedMsg[1]);
            }
            return null;
        }

        private String[] splitMessage(String msg) {
            String[] splitMsg = new String[2];
            String[] parts = msg.split("\\s+", 2);
            if (parts.length == 2) {
                splitMsg[0] = parts[0].replaceAll("[()]", "");
                splitMsg[1] = parts[1];
                return splitMsg;
            }
            return null;
        }

        private boolean badInputFormat(String msg) {
            String regex = "^\\(\\w+\\)\\s[A-Za-z0-9_?!@#$%^&*\\s]+$";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(msg);
            return !matcher.matches();
        }
    }

}