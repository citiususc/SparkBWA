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

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * This class define a custom RecordReader for a pair of FASTQ files for the Hadoop MapReduce
 * framework.
 *
 * @author José M. Abuín
 */
public class FastqRecordReaderDouble extends RecordReader<Long, String> {

  private final String[] lines = new String[8];
  private final long[] keys = new long[8];
  // input data comes from lrr
  private LineRecordReader lrr = null;
  private Long key = 0L;
  private String value = "";

  @Override
  public void close() throws IOException {
    this.lrr.close();
  }

  @Override
  public Long getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public String getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return this.lrr.getProgress();
  }

  @Override
  public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    this.lrr = new LineRecordReader();
    this.lrr.initialize(inputSplit, taskAttemptContext);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    int count = 0;
    boolean found = false;

    while (!found) {

      if (!this.lrr.nextKeyValue()) {
        return false;
      }

      final String s = this.lrr.getCurrentValue().toString().trim();
      //System.out.println("nextKeyValue() s="+s);

      // Prevent empty lines
      if (s.length() == 0) {
        continue;
      }

      this.lines[count] = s;
      this.keys[count] = this.lrr.getCurrentKey().get();

      if (count < 7) {
        count++;
      } else {
        if (this.lines[0].charAt(0) == '@'
            && this.lines[4].charAt(0) == '+'
            && this.lines[1].charAt(0) == '@'
            && this.lines[5].charAt(0) == '+') {
          found = true;
        } else {
          shiftLines();
          shiftPositions(); //this.keys[i] = this.keys[i+1];
        }
      }
    } //end-while

    // set key
    this.key = this.keys[0];
    // set value
    this.value = buildValue();
    // clear records for next FASTQ sequence
    clearRecords();

    return true;
  }

  private void shiftLines() {
    // this.lines[i] = this.lines[i+1];
    this.lines[0] = this.lines[1];
    this.lines[1] = this.lines[2];
    this.lines[2] = this.lines[3];
    this.lines[3] = this.lines[4];
    this.lines[4] = this.lines[5];
    this.lines[5] = this.lines[6];
    this.lines[6] = this.lines[7];
  }

  private void shiftPositions() {
    //this.keys[i] = this.keys[i+1];
    this.keys[0] = this.keys[1];
    this.keys[1] = this.keys[2];
    this.keys[2] = this.keys[3];

    this.keys[3] = this.keys[4];
    this.keys[4] = this.keys[5];
    this.keys[5] = this.keys[6];
    this.keys[6] = this.keys[7];
  }

  private void clearRecords() {
    this.lines[0] = null;
    this.lines[1] = null;
    this.lines[2] = null;
    this.lines[3] = null;
    this.lines[4] = null;
    this.lines[5] = null;
    this.lines[6] = null;
    this.lines[7] = null;
  }

  private String buildValue() {
    StringBuilder builder = new StringBuilder();
    builder.append(lines[0]);
    //builder.append(",;,");
    builder.append("\n");
    builder.append(lines[1]);
    //builder.append(",;,");
    builder.append("\n");
    builder.append(lines[2]);
    //builder.append(",;,");
    builder.append("\n");
    builder.append(lines[3]);

    builder.append("\n");
    builder.append(lines[4]);

    builder.append("\n");
    builder.append(lines[5]);

    builder.append("\n");
    builder.append(lines[6]);

    builder.append("\n");
    builder.append(lines[7]);

    //return new Text(builder.toString());
    return builder.toString();
  }
}
