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

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Class to perform the alignment over a split from the RDD
 *
 * @author José M. Abuín
 * @return A RDD containing the resulting Sam files from the alignment.
 */
public class BwaPairedAlignment extends BwaAlignmentBase
    implements Function2<Integer, Iterator<Tuple2<String, String>>, Iterator<String>> {

  public BwaPairedAlignment(SparkContext context, Bwa bwaInterpreter) {
    super(context, bwaInterpreter);
  }

  /**
   * Code to run in each one of the mappers. This is, the alignment with the corresponding entry
   * data The entry data has to be written into the local filesystem
   */
  public Iterator<String> call(Integer arg0, Iterator<Tuple2<String, String>> arg1) throws Exception {

    // STEP 1: Input fastq reads tmp file creation
    LOG.info("JMAbuin:: Tmp dir: " + this.tmpDir);
    String fastqFileName1 = this.tmpDir + this.appId + "-RDD" + arg0 + "_1";
    String fastqFileName2 = this.tmpDir + this.appId + "-RDD" + arg0 + "_2";

    LOG.info("JMAbuin:: Writing file: " + fastqFileName1);
    LOG.info("JMAbuin:: Writing file: " + fastqFileName2);

    File FastqFile1 = new File(fastqFileName1);
    File FastqFile2 = new File(fastqFileName2);

    FileOutputStream fos1;
    FileOutputStream fos2;

    BufferedWriter bw1;
    BufferedWriter bw2;

    ArrayList<String> returnedValues = new ArrayList<String>();

    //We write the data contained in this split into the two tmp files
    try {
      fos1 = new FileOutputStream(FastqFile1);
      fos2 = new FileOutputStream(FastqFile2);

      bw1 = new BufferedWriter(new OutputStreamWriter(fos1));
      bw2 = new BufferedWriter(new OutputStreamWriter(fos2));

      Tuple2<String, String> newFastqRead;

      while (arg1.hasNext()) {
        newFastqRead = arg1.next();

        bw1.write(newFastqRead._1);
        bw1.newLine();

        bw2.write(newFastqRead._2);
        bw2.newLine();
      }

      bw1.close();
      bw2.close();

      arg1 = null;

      returnedValues = this.runAlignmentProcess(arg0, fastqFileName1, fastqFileName2);

      // Delete temporary files, as they have now been copied to the
      // output directory
      LOG.info("JMAbuin:: Deleting file: " + fastqFileName1);
      FastqFile1.delete();
      LOG.info("JMAbuin:: Deleting file: " + fastqFileName2);
      FastqFile2.delete();

    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      LOG.error(e.toString());
    }

    return returnedValues.iterator();
  }
}
