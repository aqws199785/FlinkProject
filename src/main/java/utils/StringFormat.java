package utils;

public class StringFormat {
    public static String format(String key, Object value) {
        String formattedString = "My name is {name} and I am {age} years old";

        return "[ " + key + " " + ":" + " " + value + " " + "]";
    }

    public static void main(String[] args) {
        System.out.println(format("hello", "word"));
    }
}
