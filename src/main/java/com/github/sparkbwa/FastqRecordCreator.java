package com.github.sparkbwa;

import org.apache.spark.api.java.function.Function;

import java.util.Iterator;
import java.util.regex.Pattern;

public class FastqRecordCreator implements Function<Iterable<String>, String> {
    @Override
    public String call(Iterable<String> stringIteable) throws Exception {
        Iterator<String> stringIterator = stringIteable.iterator();
        String recordID = null;
        String bases = null;
        String plus = null;
        String scores = null;

        Pattern seq_re = Pattern.compile("[A-Za-z\\n\\.~]+");

        for (String element : stringIteable) {
            if (element.startsWith("@")) {
                recordID = element;
            } else if (element.equals("+")) {
                plus = element;
            } else if (seq_re.matcher(element).find()) {
                bases = element;
            } else {
                scores = element;
            }
        }

        if (recordID.startsWith("@") && plus.equals("+")) {
            return String.format("%s\n%s\n%s\n%s\n", recordID, bases, plus, scores);
        } else {
            System.err.println("Malformed record!");
            System.err.println(String.format("%s\n%s\n%s\n%s\n", recordID, bases, plus, scores));
            return null;
        }
    }
}
