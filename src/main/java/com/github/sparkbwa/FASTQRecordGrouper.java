package com.github.sparkbwa;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class FASTQRecordGrouper implements PairFunction<Tuple2<Long, String>, Long, Tuple2<String, Long>> {
    @Override
    public Tuple2<Long, Tuple2<String, Long>> call(Tuple2<Long, String> recordTuple) throws Exception {
        Long fastqLineNum = recordTuple._1();
        String line = recordTuple._2();

        Long recordNum = (long) Math.floor(fastqLineNum / 4);
        Tuple2<String, Long> newRecordTuple = new Tuple2<>(line, fastqLineNum % 4);

        return new Tuple2<>(recordNum, newRecordTuple);
    }
}
