/**
 * Copyright 2016 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
 *
 * <p>This file is part of SparkBWA.
 *
 * <p>SparkBWA is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * <p>SparkBWA is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * <p>You should have received a copy of the GNU General Public License along with SparkBWA. If not,
 * see <http://www.gnu.org/licenses/>.
 */

package com.github.sparkbwa;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Class that implements functionality of indexing FASTQ input lines into groups of 4 (one read)
 * @author Pål Karlsrud
 * @author Jose M. Abuin
 */
public class FASTQRecordGrouper implements PairFunction<Tuple2<String, Long>, Long, Tuple2<String, Long>> {

	@Override
	public Tuple2<Long, Tuple2<String, Long>> call(Tuple2<String, Long> recordTuple) throws Exception {

		// We get string input and line number
		String line = recordTuple._1();
		Long fastqLineNum = recordTuple._2();

		// We obtain the record number from the line number
		Long recordNum = (long) Math.floor(fastqLineNum / 4);
		// We form the pair <String line, Long index inside record (0..3)>
		Tuple2<String, Long> newRecordTuple = new Tuple2<String, Long>(line, fastqLineNum % 4);

		// We return the data to group <Long record number,<String line, Long line index>>
		return new Tuple2<Long,Tuple2<String, Long>>(recordNum, newRecordTuple);
	}
}
