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

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * Class that implements functionality of grouping FASTQ indexed PairRDD
 * @author Pål Karlsrud
 * @author Jose M. Abuin
 */
public class FASTQRecordCreator implements Function<Iterable<Tuple2<String, Long>>, String> {

	@Override
	public String call(Iterable<Tuple2<String, Long>> iterableRecord) throws Exception {
		// We create the data to be contained inside the record
		String seqName 		= null;
		String seq			= null;
		String qual			= null;
		String extraSeqname	= null;

		//
		for (Tuple2<String, Long> recordLine : iterableRecord) {
			// Keep in mind that records are sorted by key. This is, we have 4 available lines here
			Long lineNum = recordLine._2();
			String line = recordLine._1();

			if (lineNum == 0) {
				seqName = line;
			}
			else if (lineNum == 1) {
				seq = line;
			}
			else if (lineNum == 2) {
				extraSeqname = line;
			}
			else {
				qual = line;
			}
		}

		// If everything went fine, we return the current record
		if (seqName != null && seq != null && qual != null && extraSeqname != null) {
			return String.format("%s\n%s\n%s\n%s\n", seqName, seq, extraSeqname, qual);
		}
		else {
			System.err.println("Malformed record!");
			System.err.println(String.format("%s\n%s\n%s\n%s\n", seqName, seq, extraSeqname, qual));
			return null;
		}
	}
}
