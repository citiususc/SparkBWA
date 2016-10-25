package com.github.sparkbwa;

import com.clearspring.analytics.util.Lists;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Pattern;

public class FASTQRecordCreator implements Function<Iterable<Tuple2<String, Long>>, String> {
    @Override
    public String call(Iterable<Tuple2<String, Long>> iterableRecord) throws Exception {
        String seqName = null;
        String seq = null;
        String qual = null;
        String extraSeqname = null;

        for (Tuple2<String, Long> recordLine : iterableRecord) {
            Long lineNum = recordLine._2();
            String line = recordLine._1();

            if (lineNum == 0)
                seqName = line;
            else if (lineNum == 1)
                seq = line;
            else if (lineNum == 2)
                extraSeqname = line;
            else
                qual = line;
        }

        if (seqName != null && seq != null && qual != null && extraSeqname != null) {
            return String.format("%s\n%s\n%s\n%s\n", seqName, seq, extraSeqname, qual);
        } else {
            System.err.println("Malformed record!");
            System.err.println(String.format("%s\n%s\n%s\n%s\n", seqName, seq, extraSeqname, qual));
            return null;
        }
    }
}
