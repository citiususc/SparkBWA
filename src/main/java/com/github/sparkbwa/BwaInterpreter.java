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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.ContextCleaner;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

/**
 * BwaInterpreter class
 *
 * @author José M. Abuín
 * @brief This class communicates Spark with BWA
 */
public class BwaInterpreter {

  private static final Log LOG = LogFactory.getLog(BwaInterpreter.class);
  private SparkConf sparkConf;
  private JavaSparkContext ctx;
  private Configuration conf;
  private JavaRDD<Tuple2<String, String>> dataRDD;
  private long totalInputLength;
  private long blocksize;
  private BwaOptions options;
  private String inputTmpFileName;

  /**
   * Constructor to build the BwaInterpreter object from the Spark shell When creating a
   * BwaInterpreter object from the Spark shell, the BwaOptions and the Spark Context objects need
   * to be passed as argument.
   *
   * @param opcions The BwaOptions object initialized with the user options
   * @param context The Spark Context from the Spark Shell. Usually "sc"
   * @return The BwaInterpreter object with its options initialized.
   */
  public BwaInterpreter(BwaOptions opcions, SparkContext context) {

    this.options = opcions;
    this.ctx = new JavaSparkContext(context);
    this.initInterpreter();
  }

  /**
   * Constructor to build the BwaInterpreter object from within SparkBWA
   *
   * @param args Arguments got from Linux console when launching SparkBWA with Spark
   * @return The BwaInterpreter object with its options initialized.
   */
  public BwaInterpreter(String[] args) {

    this.options = new BwaOptions(args);
    this.initInterpreter();
  }

  private void setTotalInputLength() {
    try {
      FileSystem fs = FileSystem.get(this.conf);

      // To get the input files sizes
      ContentSummary cSummaryFile1 = fs.getContentSummary(new Path(options.getInputPath()));

      long lengthFile1 = cSummaryFile1.getLength();
      long lengthFile2 = 0;

      if (!options.getInputPath2().isEmpty()) {
        ContentSummary cSummaryFile2 = fs.getContentSummary(new Path(options.getInputPath()));
        lengthFile2 = cSummaryFile2.getLength();
      }

      // Total size. Depends on paired or single reads
      this.totalInputLength = lengthFile1 + lengthFile2;
      fs.close();
    } catch (IOException e) {
      LOG.error(e.toString());
      e.printStackTrace();
    }
  }

  private void createOutputFolder() {
    try {
      FileSystem fs = FileSystem.get(this.conf);

      // Path variable
      Path outputDir = new Path(options.getOutputPath());

      // Directory creation
      if (!fs.exists(outputDir)) {
        fs.mkdirs(outputDir);
      } else {
        fs.delete(outputDir, true);
        fs.mkdirs(outputDir);
      }

      fs.close();
    } catch (IOException e) {
      LOG.error(e.toString());
      e.printStackTrace();
    }
  }

  private JavaRDD<String> handleSingleReadsSorting() {
    JavaRDD<String> readsRDD = null;

    long startTime = System.nanoTime();

    LOG.info("JMAbuin::Not sorting in HDFS. Timing: " + startTime);

    // Read the two FASTQ files from HDFS using the FastqInputFormat class
    JavaPairRDD<Long, String> singleReadsKeyVal = loadFastq(this.ctx, this.options.getInputPath());

    // Sort in memory with no partitioning
    if ((options.getPartitionNumber() == 0) && (options.isSortFastqReads())) {
      // First, the join operation is performed. After that,
      // a sortByKey. The resulting values are obtained
      readsRDD = singleReadsKeyVal.sortByKey().values();
      LOG.info("JMAbuin:: Sorting in memory without partitioning");
    }

    // Sort in memory with partitioning
    else if ((options.getPartitionNumber() != 0) && (options.isSortFastqReads())) {
      singleReadsKeyVal = singleReadsKeyVal.repartition(options.getPartitionNumber());
      readsRDD = singleReadsKeyVal.sortByKey().values().persist(StorageLevel.MEMORY_ONLY());
      LOG.info("JMAbuin:: Repartition with sort");
    }

    // No Sort with no partitioning
    else if ((options.getPartitionNumber() == 0) && (!options.isSortFastqReads())) {
      LOG.info("JMAbuin:: No sort and no partitioning");
      readsRDD = singleReadsKeyVal.values();
    }

    // No Sort with partitioning
    else {
      LOG.info("JMAbuin:: No sort with partitioning");
      int numPartitions = singleReadsKeyVal.partitions().size();

      /*
       * As in previous cases, the coalesce operation is not suitable
       * if we want to achieve the maximum speedup, so, repartition
       * is used.
       */
      if ((numPartitions) <= options.getPartitionNumber()) {
        LOG.info("JMAbuin:: Repartition with no sort");
      } else {
        LOG.info("JMAbuin:: Repartition(Coalesce) with no sort");
      }

      readsRDD =
          singleReadsKeyVal
              .repartition(options.getPartitionNumber())
              .values()
              .persist(StorageLevel.MEMORY_ONLY());
      long endTime = System.nanoTime();

      LOG.info("JMAbuin:: End of sorting. Timing: " + endTime);
      LOG.info("JMAbuin:: Total time: " + (endTime - startTime) / 1e9 / 60.0 + " minutes");
    }

    readsRDD.persist(StorageLevel.MEMORY_ONLY());

    return readsRDD;
  }

  public static JavaPairRDD<Long, String> loadFastq(JavaSparkContext ctx, String pathToFastq) {
    JavaRDD<String> fastqLines = ctx.textFile(pathToFastq);

    // Determine which FASTQ record the line belongs to.
    JavaPairRDD<Long, Tuple2<String, Long>> fastqLinesByRecordNum = fastqLines.zipWithIndex().mapToPair(new FASTQRecordGrouper());

    // Group group the lines which belongs to the same record, and concatinate them into a record.
    return fastqLinesByRecordNum.groupByKey().mapValues(new FASTQRecordCreator());
  }

  private JavaRDD<Tuple2<String, String>> handlePairedReadsSorting() {
    JavaRDD<Tuple2<String, String>> readsRDD = null;

    long startTime = System.nanoTime();

    LOG.info("JMAbuin::Not sorting in HDFS. Timing: " + startTime);

    // Read the two FASTQ files from HDFS using the FastqInputFormat class
    JavaPairRDD<Long, String> datasetTmp1 = loadFastq(this.ctx, options.getInputPath());
    JavaPairRDD<Long, String> datasetTmp2 = loadFastq(this.ctx, options.getInputPath2());
    JavaPairRDD<Long, Tuple2<String, String>> pairedReadsRDD = datasetTmp1.join(datasetTmp2);

    datasetTmp1.unpersist();
    datasetTmp2.unpersist();

    // Sort in memory with no partitioning
    if ((options.getPartitionNumber() == 0) && (options.isSortFastqReads())) {
      // First, the join operation is performed. After that,
      // a sortByKey. The resulting values are obtained
      readsRDD = pairedReadsRDD.sortByKey().values();
      LOG.info("JMAbuin:: Sorting in memory without partitioning");
    }

    // Sort in memory with partitioning
    else if ((options.getPartitionNumber() != 0) && (options.isSortFastqReads())) {
      pairedReadsRDD = pairedReadsRDD.repartition(options.getPartitionNumber());
      readsRDD = pairedReadsRDD.sortByKey().values().persist(StorageLevel.MEMORY_ONLY());
      LOG.info("JMAbuin:: Repartition with sort");
    }

    // No Sort with no partitioning
    else if ((options.getPartitionNumber() == 0) && (!options.isSortFastqReads())) {
      LOG.info("JMAbuin:: No sort and no partitioning");
    }

    // No Sort with partitioning
    else {
      LOG.info("JMAbuin:: No sort with partitioning");
      int numPartitions = pairedReadsRDD.partitions().size();

      /*
       * As in previous cases, the coalesce operation is not suitable
       * if we want to achieve the maximum speedup, so, repartition
       * is used.
       */
      if ((numPartitions) <= options.getPartitionNumber()) {
        LOG.info("JMAbuin:: Repartition with no sort");
      } else {
        LOG.info("JMAbuin:: Repartition(Coalesce) with no sort");
      }

      readsRDD =
          pairedReadsRDD
              .repartition(options.getPartitionNumber())
              .values()
              .persist(StorageLevel.MEMORY_ONLY());
    }

    long endTime = System.nanoTime();

    LOG.info("JMAbuin:: End of sorting. Timing: " + endTime);
    LOG.info("JMAbuin:: Total time: " + (endTime - startTime) / 1e9 / 60.0 + " minutes");
    readsRDD.persist(StorageLevel.MEMORY_ONLY());

    return readsRDD;
  }

  /**
   * Procedure to perform the alignment
   *
   * @author José M. Abuín
   */
  private List<String> MapPairedBwa(Bwa bwa, JavaRDD<Tuple2<String, String>> readsRDD) {
    // The mapPartitionsWithIndex is used over this RDD to perform the alignment. The resulting sam filenames are returned
    return readsRDD
        .mapPartitionsWithIndex(new BwaPairedAlignment(readsRDD.context(), bwa), true)
        .collect();
  }

  private List<String> MapSingleBwa(Bwa bwa, JavaRDD<String> readsRDD) {
    // The mapPartitionsWithIndex is used over this RDD to perform the alignment. The resulting sam filenames are returned
    return readsRDD
        .mapPartitionsWithIndex(new BwaSingleAlignment(readsRDD.context(), bwa), true)
        .collect();
  }

  /**
   * Runs BWA with the specified options
   *
   * @brief This function runs BWA with the input data selected and with the options also selected
   *     by the user.
   */
  public void runBwa() {
    LOG.info("JMAbuin:: Starting BWA");
    Bwa bwa = new Bwa(this.options);

    List<String> returnedValues;
    if (bwa.isPairedReads()) {
      JavaRDD<Tuple2<String, String>> readsRDD = handlePairedReadsSorting();
      returnedValues = MapPairedBwa(bwa, readsRDD);
    } else {
      JavaRDD<String> readsRDD = handleSingleReadsSorting();
      returnedValues = MapSingleBwa(bwa, readsRDD);
    }

    // In the case of use a reducer the final output has to be stored in just one file
    for (String outputFile : returnedValues) {
      LOG.info("JMAbuin:: SparkBWA:: Returned file ::" + outputFile);

      //After the execution, if the inputTmp exists, it should be deleted
      try {
        if ((this.inputTmpFileName != null) && (!this.inputTmpFileName.isEmpty())) {
          FileSystem fs = FileSystem.get(this.conf);

          fs.delete(new Path(this.inputTmpFileName), true);

          fs.close();
        }

      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        LOG.error(e.toString());
      }
    }
  }

  /**
   * Procedure to init the BwaInterpreter configuration parameters
   *
   * @author José M. Abuín
   */
  public void initInterpreter() {
    //If ctx is null, this procedure is being called from the Linux console with Spark
    if (this.ctx == null) {

      String sorting;

      //Check for the options to perform the sort reads
      if (options.isSortFastqReads()) {
        sorting = "SortSpark";
      } else if (options.isSortFastqReadsHdfs()) {
        sorting = "SortHDFS";
      } else {
        sorting = "NoSort";
      }
      //The application name is set
      this.sparkConf =
          new SparkConf()
              .setAppName(
                  "SparkBWA_"
                      + options.getInputPath().split("/")[
                          options.getInputPath().split("/").length - 1]
                      + "-"
                      + options.getPartitionNumber()
                      + "-"
                      + sorting);
      //The ctx is created from scratch
      this.ctx = new JavaSparkContext(this.sparkConf);

    }
    //Otherwise, the procedure is being called from the Spark shell
    else {

      this.sparkConf = this.ctx.getConf();
    }
    //The Hadoop configuration is obtained
    this.conf = this.ctx.hadoopConfiguration();

    //The block size
    this.blocksize = this.conf.getLong("dfs.blocksize", 134217728);

    createOutputFolder();
    setTotalInputLength();

    ContextCleaner cleaner = this.ctx.sc().cleaner().get();
  }
}
