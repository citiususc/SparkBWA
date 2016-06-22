/**
 * Copyright 2016 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
 * 
 * This file is part of SparkBWA.
 *
 * SparkBWA is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * SparkBWA is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SparkBWA. If not, see <http://www.gnu.org/licenses/>.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.ContextCleaner;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

/**
 * BwaInterpreter class
 * @author José M. Abuín
 * @brief This class communicates Spark with BWA
 */
public class BwaInterpreter {

	private SparkConf sparkConf; 			/**< The Spark configuration */
	private JavaSparkContext ctx;			/**< The Java Spark Context */
	private Configuration conf;				/**< The Configuration */


	private BwaRDD dataRDD;					/**< RDD implemented from the BwaRDD class that contains the reads to be processed */

	private long totalInputLength;			/**< To store the length of the input data */
	private long blocksize;					/**< To store the block size in HDFS */

	private static final Log LOG = LogFactory.getLog(BwaInterpreter.class); /**< The Log */

	private BwaOptions options;				/**< Options to launch BWA */

	private String inputTmpFileName;		/**< String containing input file name */

    /**
     * Constructor to build the BwaInterpreter object from the Spark shell
     * When creating a BwaInterpreter object from the Spark shell, the BwaOptions and the Spark Context objects need to be passed as argument.
     * @param opcions The BwaOptions object initialized with the user options
     * @param context The Spark Context from the Spark Shell. Usually "sc"
     * @return The BwaInterpreter object with its options initialized.
     */
	public BwaInterpreter(BwaOptions opcions, SparkContext context){

		this.options = opcions;
		this.ctx = new JavaSparkContext(context);
		this.initInterpreter();
	}

	/**
	 * Constructor to build the BwaInterpreter object from within SparkBWA
	 * @param args Arguments got from Linux console when launching SparkBWA with Spark
	 * @return The BwaInterpreter object with its options initialized.
	 */
	public BwaInterpreter(String[] args){


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

			if(!options.getInputPath2().isEmpty()){
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
			if(!fs.exists(outputDir)){
				fs.mkdirs(outputDir);
			}
			else{
				fs.delete(outputDir, true);
				fs.mkdirs(outputDir);
			}

			fs.close();
		} catch (IOException e) {
			LOG.error(e.toString());
			e.printStackTrace();
		}
	}

	private void setSparkSettings() {
		// Some configuration options are set. However, the option to load the
		// bwa library needs to be specified in the Spark configuration files,
		// because these options does not work (neither of them)
		this.sparkConf.set("spark.yarn.dist.archives","./bwa.zip");
		this.conf.set("mapreduce.map.env", "LD_LIBRARY_PATH=./bwa.zip/");
		this.conf.set("mapreduce.reduce.env", "LD_LIBRARY_PATH=./bwa.zip/");
		this.sparkConf.set("spark.driver.extraLibraryPath", "./bwa.zip/");
		this.sparkConf.set("spark.executor.extraLibraryPath", "./bwa.zip/");
		this.sparkConf.set("spark.executor.extraJavaOptions", "-Djava.library.path=./bwa.zip/");
	}

	/**
	 * Procedure to init the BwaInterpreter configuration parameters
	 * @author José M. Abuín
	 */
	public void initInterpreter(){
		//If ctx is null, this procedure is being called from the Linux console with Spark
		if(this.ctx == null){

			String sorting;

			//Check for the options to perform the sort reads
			if(options.isSortFastqReads()){
				sorting = "SortSpark";
			}
			else if(options.isSortFastqReadsHdfs()){
				sorting = "SortHDFS";
			}
			else{
				sorting = "NoSort";
			}
			//The application name is set
			this.sparkConf = new SparkConf().setAppName("SparkBWA_"+options.getInputPath().split("/")[options.getInputPath().split("/").length-1]+"-"+options.getPartitionNumber()+"-"+sorting);

			//The ctx is created from scratch
			this.ctx = new JavaSparkContext(this.sparkConf);

		}
		//Otherwise, the procedure is being called from the Spark shell
		else{

			this.sparkConf = this.ctx.getConf();

		}
		//The Hadoop configuration is obtained
		this.conf = this.ctx.hadoopConfiguration();

		//The block size
		this.blocksize = this.conf.getLong("dfs.blocksize", 134217728);

		createOutputFolder();
		setTotalInputLength();

		setSparkSettings();

		ContextCleaner cleaner = this.ctx.sc().cleaner().get();

		//The two RDDs to store input reads from FASTQ files in HDFS
		JavaPairRDD<Long,String> datasetTmp1 = null;
		JavaPairRDD<Long,String> datasetTmp2 = null;

		//RDD where paired reads are going to be stored under the same key
		JavaRDD<Tuple2<String,String>> pairedDataRDD = null;

		LOG.info("JMAbuin:: Starting sorting if desired");

		//Not sorting in HDFS
		if(!options.isSortFastqReadsHdfs()){

			long startTime = System.nanoTime();

			LOG.info("JMAbuin::Not sorting in HDFS. Timing: "+startTime);

			//Read the two FASTQ files from HDFS using the FastqInputFormat class
			datasetTmp1 = ctx.newAPIHadoopFile(options.getInputPath(), FastqInputFormat.class, Long.class, String.class, this.conf).persist(StorageLevel.MEMORY_ONLY());
			datasetTmp2 = ctx.newAPIHadoopFile(options.getInputPath2(), FastqInputFormat.class, Long.class, String.class,this.conf).persist(StorageLevel.MEMORY_ONLY());

			//Sort in memory with no partitioning
			if((options.getPartitionNumber()==0) && (options.isSortFastqReads())){

				//First, the join operation is performed. After that, a sortByKey. The resulting values are obtained
				pairedDataRDD = datasetTmp1.join(datasetTmp2).sortByKey().values();
				LOG.info("JMAbuin:: Sorting in memory without partitioning");
			}

			//Sort in memory with partitioning
			else if((options.getPartitionNumber()!=0) && (options.isSortFastqReads())){
				//The first step is perform the join between the two FASTQ files
				JavaPairRDD<Long,Tuple2<String,String>> tmpRDD = datasetTmp1.join(datasetTmp2).persist(StorageLevel.MEMORY_ONLY());

				tmpRDD = tmpRDD.repartition(options.getPartitionNumber());
				pairedDataRDD = tmpRDD.sortByKey().values().persist(StorageLevel.MEMORY_ONLY());
				LOG.info("JMAbuin:: Repartition with sort");

				tmpRDD.unpersist();
			}

			//No Sort with no partitioning
			else if((options.getPartitionNumber()==0) && (!options.isSortFastqReads())){
				//In this case only the join has to be performed
				pairedDataRDD = datasetTmp1.join(datasetTmp2).values();
				LOG.info("JMAbuin:: No sort and no partitioning");

			}

			//No Sort with partitioning
			else{
				LOG.info("JMAbuin:: No sort with partitioning");

				//Again, the fisrt step is perform the join operation
				JavaPairRDD<Long,Tuple2<String,String>> tmpRDD = datasetTmp1.join(datasetTmp2).persist(StorageLevel.MEMORY_ONLY());

				int numPartitions = tmpRDD.partitions().size();

				/*
				 * As in previous cases, the coalesce operation is not suitable if we want to achieve the maximum speedup, so, repartition is used.
				 */
				if((numPartitions) <= options.getPartitionNumber()){
					pairedDataRDD = tmpRDD.repartition(options.getPartitionNumber()).values().persist(StorageLevel.MEMORY_ONLY());
					LOG.info("JMAbuin:: Repartition with no sort");
				}
				else{
					pairedDataRDD = tmpRDD.repartition(options.getPartitionNumber()).values().persist(StorageLevel.MEMORY_ONLY());
					LOG.info("JMAbuin:: Repartition(Coalesce) with no sort");
				}

				tmpRDD.unpersist();


			}

			datasetTmp1.unpersist();
			datasetTmp2.unpersist();

			cleaner.doCleanupRDD(datasetTmp1.id(), true);
			cleaner.doCleanupRDD(datasetTmp2.id(), true);

			long endTime = System.nanoTime();

			LOG.info("JMAbuin:: End of sorting. Timing: "+endTime);
			LOG.info("JMAbuin:: Total time: "+(endTime-startTime)/1e9/60.0+" minutes");

		}

		//Sorting in HDFS
		else{
			long startTime;
			long endTime;

			//The temp file name
			this.inputTmpFileName = options.getInputPath().split("/")[options.getInputPath().split("/").length-1]+"-"+options.getInputPath2().split("/")[options.getInputPath2().split("/").length-1];

			startTime = System.nanoTime();

			LOG.info("JMAbuin:: Sorting in HDFS. Start time: "+startTime);

			//The SortInHDFS2 function is used. It returns the corresponding RDD
			//pairedDataRDD = this.SortInHDFS(options.getInputPath(), options.getInputPath2());
			pairedDataRDD = this.SortInHDFS2(options.getInputPath(), options.getInputPath2());//.persist(StorageLevel.MEMORY_ONLY());

			endTime = System.nanoTime();
			LOG.info("JMAbuin:: End of sorting. Timing: "+endTime);
			LOG.info("JMAbuin:: Total time: "+(endTime-startTime)/1e9/60.0+" minutes");
		}

		datasetTmp1 = null;
		datasetTmp2 = null;

		//After processing the input FASTQ reads, the BwaRDD is created
		this.dataRDD = new BwaRDD(pairedDataRDD.rdd(),pairedDataRDD.classTag());

		//Also, the BWA options are set
		this.dataRDD.setOptions(options);
		this.dataRDD.persist(StorageLevel.MEMORY_ONLY());
	}


	/**
	 * Runs BWA with the specified options
	 * @brief This function runs BWA with the input data selected and with the options also selected by the user.
	 */
	public void RunBwa(){

		LOG.info("JMAbuin:: Starting BWA");

		//The function to actually run BWA is inside the BwaRDD class.
		this.dataRDD.MapBwa();

		//After the execution, if the inputTmp exists, it should be deleted
		try {

			if( (this.inputTmpFileName!= null) && (!this.inputTmpFileName.isEmpty())){
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

	/**
	 * This function only calls RunBwa
	 */
	public void runAlignment(){

		this.RunBwa();
	}

	/**
	 * Used to perform the sort operation in HDFS
	 * @brief This function provides a method to perform the sort phase in HDFS
	 * @author José M. Abuín
	 * @param fileName1 The first file that contains input FASTQ reads. Stored in HDFS
	 * @param fileName2 The second file that contains input FASTQ reads. Stored in HDFS
	 * @return A JavaRDD that contains the paired reads sorted
	 */
	public JavaRDD<Tuple2<String,String>> SortInHDFS2(String fileName1, String fileName2){

		Configuration conf = this.conf;

		LOG.info("JMAbuin:: Starting writing reads to HDFS");

		try {
			FileSystem fs = FileSystem.get(conf);


			Path outputFilePath = new Path(this.inputTmpFileName);

			//To write the paired reads
			FSDataOutputStream outputFinalStream = fs.create(outputFilePath, true);

			//To read paired reads from both files
			BufferedReader brFastqFile1 = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName1))));
			BufferedReader brFastqFile2 = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName2))));

			String lineFastq1;
			String lineFastq2;

			lineFastq1 = brFastqFile1.readLine();
			lineFastq2 = brFastqFile2.readLine();

			//Loop to read two files. The two of them must have the same line numbers
			while (lineFastq1 != null){
				//The lines are written interspersed
				outputFinalStream.write((lineFastq1+"\n"+lineFastq2+"\n").getBytes());

				//Next lines are readed
				lineFastq1 = brFastqFile1.readLine();
				lineFastq2 = brFastqFile2.readLine();
			}

			//Close the input and output files
			brFastqFile1.close();
			brFastqFile2.close();
			outputFinalStream.close();

			//Now it is time to read the previous created file and create the RDD
			ContentSummary cSummary = fs.getContentSummary(outputFilePath);

			long length = cSummary.getLength();

			this.totalInputLength = length;

			fs.close();

			//In case of the user does want partitioning
			if(this.options.getPartitionNumber()!=0){

				//These options are set to indicate the split size and get the correct vnumber of partitions
				this.conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf((length)/this.options.getPartitionNumber()));
				this.conf.set("mapreduce.input.fileinputformat.split.minsize", String.valueOf((length)/this.options.getPartitionNumber()));

				LOG.info("JMAbuin partitioning from HDFS:: "+String.valueOf((length)/this.options.getPartitionNumber()));

				//Using the FastqInputFormatDouble class we get values from the HDFS file. After that, these values are stored in a RDD
				return this.ctx.newAPIHadoopFile(this.inputTmpFileName, FastqInputFormatDouble.class, Long.class, String.class, this.conf).mapPartitions(new BigFastq2RDDPartitionsDouble(),true);

			}
			else{
				//Using the FastqInputFormatDouble class we get values from the HDFS file. After that, these values are stored in a RDD
				return this.ctx.newAPIHadoopFile(this.inputTmpFileName, FastqInputFormatDouble.class, Long.class, String.class, this.conf).map(new BigFastq2RDDDouble());
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error(e.toString());

			return null;
		} 
	}

	/**
	 * Class used to create an RDD with the map function from a file from HDFS
	 * @author José M. Abuín
	 * @return A Tuple2<String,String> RDD where each one of the Strings is one FASTQ read. The other String it is its mate.
	 */
	public static class BigFastq2RDDDouble implements Function<Tuple2<Long,String>,Tuple2<String,String>>{

		private static final long serialVersionUID = 1L;

		/*
		 * The reads from the HDFS file are stored as follows
		 * Line 0 - Line 0 from first read
		 * Line 1 - Line 0 from second read
		 * Line 2 - Line 1 from first read
		 * Line 3 - Line 1 from second read
		 * Line 4 - Line 2 from first read
		 * Line 5 - Line 2 from second read
		 * Line 6 - Line 3 from first read
		 * Line 7 - Line 3 from second read
		 */

		@Override
		public Tuple2<String, String> call(Tuple2<Long, String> arg0) throws Exception {
			String reads[] = arg0._2.split("\n");

			String record1[] = {reads[0],reads[2],reads[4],reads[6]};
			String record2[] = {reads[1],reads[3],reads[5],reads[7]};


			String value1 = record1[0]+"\n"+record1[1]+"\n"+record1[2]+"\n"+record1[3];
			String value2 = record2[0]+"\n"+record2[1]+"\n"+record2[2]+"\n"+record2[3];

			return new Tuple2<String,String>(value1,value2);
		}


	}

	/**
	 * Class used to create an RDD with the mapPartitions function from a file from HDFS
	 * @author José M. Abuín
	 * @return A Tuple2<String,String> RDD where each one of the Strings is one FASTQ read. The other String it is its mate.
	 */
	public static class BigFastq2RDDPartitionsDouble implements FlatMapFunction<Iterator<Tuple2<Long,String>>,Tuple2<String,String>>{

		private static final long serialVersionUID = 1L;

		/*
		 * The reads from the HDFS file are stored as follows
		 * Line 0 - Line 0 from first read
		 * Line 1 - Line 0 from second read
		 * Line 2 - Line 1 from first read
		 * Line 3 - Line 1 from second read
		 * Line 4 - Line 2 from first read
		 * Line 5 - Line 2 from second read
		 * Line 6 - Line 3 from first read
		 * Line 7 - Line 3 from second read
		 */

		@Override
		public Iterable<Tuple2<String, String>> call(Iterator<Tuple2<Long, String>> arg0) throws Exception {
			Tuple2<Long, String> entry;

			ArrayList<Tuple2<String,String>> returnValue = new ArrayList<Tuple2<String,String>>();
			while(arg0.hasNext()){

				entry = arg0.next();

				String reads[] = entry._2.split("\n");


				String record1[] = {reads[0],reads[2],reads[4],reads[6]};
				String record2[] = {reads[1],reads[3],reads[5],reads[7]};


				String value1 = record1[0]+"\n"+record1[1]+"\n"+record1[2]+"\n"+record1[3];
				String value2 = record2[0]+"\n"+record2[1]+"\n"+record2[2]+"\n"+record2[3];

				returnValue.add(new Tuple2<String,String>(value1,value2));
			}

			return returnValue;
		}
	}


	/**
	 * Function that returns the BwaRDD
	 * @author José M. Abuín
	 * @return The BwaRDD that contains the FASTQ reads
	 */
	public BwaRDD getDataRDD() {
		return this.dataRDD;
	}

	/**
	 * Function that sets the BwaRDD that contains the FASTQ reads
	 * @author José M. Abuín
	 * @param dataRDD The RDD containing the FASTQ reads
	 */
	public void setDataRDD(BwaRDD dataRDD) {
		this.dataRDD = dataRDD;
	}
}
