import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EmailValidator {
    public static void main(String[] args) {
        String emailAddress = "test@example.com";  // Replace with your email address

        if (isValidEmail(emailAddress)) {
            System.out.println("The provided string is a valid email address.");
        } else {
            System.out.println("The provided string is not a valid email address.");
        }
    }

    private static boolean isValidEmail(String email) {
        if (email == null || email.isEmpty()) {
            return false;  // Null or empty strings are not valid email addresses
        }

        // Define a simple regex pattern for basic email validation
        String emailRegex = "^[a-zA-Z0-9_+&*-]+(?:\\.[a-zA-Z0-9_+&*-]+)*@(?:[a-zA-Z0-9-]+\\.)+[a-zA-Z]{2,7}$";

        Pattern pattern = Pattern.compile(emailRegex);
        Matcher matcher = pattern.matcher(email);

        return matcher.matches();
    }
}
