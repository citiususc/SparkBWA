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

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

public class BwaSingleAlignment extends BwaAlignmentBase
    implements Function2<Integer, Iterator<Text>, Iterator<String>> {

  public BwaSingleAlignment(SparkContext context, Bwa bwaInterpreter) {
    super(context, bwaInterpreter);
  }

  /**
   * Code to run in each one of the mappers. This is, the alignment with the corresponding entry
   * data The entry data has to be written into the local filesystem
   */
  public Iterator<String> call(Integer arg0, Iterator<Text> arg1) throws Exception {

    LOG.info("JMAbuin:: Tmp dir: " + this.tmpDir);
    String fastqFileName1 = this.tmpDir + this.appId + "-RDD" + arg0 + "_1";

    LOG.info("JMAbuin:: Writing file: " + fastqFileName1);

    File FastqFile1 = new File(fastqFileName1);
    FileOutputStream fos1;
    BufferedWriter bw1;

    ArrayList<String> returnedValues = new ArrayList<String>();

    try {
      fos1 = new FileOutputStream(FastqFile1);
      bw1 = new BufferedWriter(new OutputStreamWriter(fos1));

      Text newFastqRead;

      while (arg1.hasNext()) {
        newFastqRead = arg1.next();

        bw1.write(newFastqRead.toString());
        bw1.newLine();
      }

      bw1.close();

      //We do not need the input data anymore, as it is written in a local file
      arg1 = null;

      returnedValues = this.runAlignmentProcess(arg0, fastqFileName1, null);
      // Delete the temporary file, as is have now been copied to the
      // output directory
      FastqFile1.delete();

    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      LOG.error(e.toString());
    }

    return returnedValues.iterator();
  }
}
