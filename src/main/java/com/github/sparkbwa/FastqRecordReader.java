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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * This class define a custom RecordReader for FASTQ files for the Hadoop MapReduce framework.
 *
 * @author Mahmoud Parsian
 * @author José M. Abuín
 */
public class FastqRecordReader extends RecordReader<LongWritable, Text> {

  private static final int MAX_LINE_LENGTH = 10000;
  private final int N_LINES_TO_PROCESS = 4; // As the FASTQ format consists of four lines.
  private LineReader in;
  private LongWritable key;
  private Text value = new Text();
  private Text tmpBuffer = new Text();
  private long start = 0;
  private long end = 0;
  private long pos = 0;
  private int maxLineLength;

  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float) (end - start));
    }
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    FileSplit split = (FileSplit) inputSplit;
    final Path file = split.getPath();
    Configuration conf = taskAttemptContext.getConfiguration();
    this.maxLineLength =
        conf.getInt("mapreduce.input.linerecordreader.line.maxlength", Integer.MAX_VALUE);
    FileSystem fs = file.getFileSystem(conf);
    start = split.getStart();
    end = start + split.getLength();
    FSDataInputStream filein = fs.open(split.getPath());

    positionAtFirstRecord(filein);

    in = new LineReader(filein, conf);
  }

  private void positionAtFirstRecord(FSDataInputStream stream) throws IOException {
    if (start > 0) {
      // Advance to the start of the first record
      // We use a temporary LineReader to read lines until we find the
      // position of the right one.  We then seek the file to that position.
      stream.seek(start);
      LineReader reader = new LineReader(stream);

      int bytesRead = 0;
      do {
        bytesRead = reader.readLine(tmpBuffer, (int) Math.min(MAX_LINE_LENGTH, end - start));
        if (bytesRead > 0 && (tmpBuffer.getLength() <= 0 || tmpBuffer.getBytes()[0] != '@'))
          start += bytesRead;
        else {
          // The line starts with @.  Read two more and verify that it starts with a '+'
          // If this isn't the start of a record, we want to backtrack to its end
          long backtrackPosition = start + bytesRead;
          bytesRead = reader.readLine(tmpBuffer, (int) Math.min(MAX_LINE_LENGTH, end - start));
          bytesRead = reader.readLine(tmpBuffer, (int) Math.min(MAX_LINE_LENGTH, end - start));

          if (bytesRead > 0 && tmpBuffer.getLength() > 0 && tmpBuffer.getBytes()[0] == '+')
            break; // all good!
          else {
            // backtrack to the end of the record we thought was the start.
            start = backtrackPosition;
            stream.seek(start);
            reader = new LineReader(stream);
          }
        }
      } while (bytesRead > 0);

      stream.seek(start);
    }
    // else
    //  if start == 0 we presume it starts with a valid fastq record
    pos = start;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    if (key == null) {
      key = new LongWritable();
    }

    if (value == null) {
      value = new Text();
    }
    value.clear();

    final Text endline = new Text("\n");
    int newSize = 0;

    for (int i = 0; i < N_LINES_TO_PROCESS; i++) {
      Text v = new Text();
      while (pos < end) {
        // Get the size of the new line
        newSize =
            in.readLine(
                v,
                maxLineLength,
                Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));

        // Read the actual lines
        value.append(v.getBytes(), 0, v.getLength());

        // Add a newline to the end of the file
        value.append(endline.getBytes(), 0, endline.getLength());
        if (newSize == 0) {
          break;
        }

        pos += newSize;
        if (newSize < maxLineLength) {
          break;
        }
      }
    }

    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    } else {
      return true;
    }
  }
}
