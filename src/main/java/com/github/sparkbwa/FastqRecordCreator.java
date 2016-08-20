package com.github.sparkbwa;

import org.apache.spark.api.java.function.Function;

import java.util.Iterator;

public class FastqRecordCreator implements Function<Iterable<String>, String> {
    @Override
    public String call(Iterable<String> stringIteable) throws Exception {
        Iterator<String> stringIterator = stringIteable.iterator();
        String recordID = stringIterator.next();
        String bases = stringIterator.next();
        String plus = stringIterator.next();
        String scores = stringIterator.next();

        if (recordID.startsWith("@") && plus.equals("+")) {
            return String.format("%s\n%s\n%s\n%s\n", recordID, bases, plus, scores);
        } else {
            System.err.println("Malformed record!");
            System.err.println(String.format("%s\n%s\n%s\n%s\n", recordID, bases, plus, scores));
            return null;
        }
    }
}
