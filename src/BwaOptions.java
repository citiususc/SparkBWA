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

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
//import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Class to parse and set the Bwa options
 * @author José M. Abuín
 *
 */
public class BwaOptions {


	private boolean useReducer 			= false;	/**< The option to use the reduce phase */

	//Algorithms boolean variables
	private boolean memAlgorithm 		= true;		/**< The option to use the MEM algorithm */
	private boolean alnAlgorithm 		= false;	/**< The option to use the ALN algorithm */
	private boolean bwaswAlgorithm 		= false;	/**< The option to use the BWASW algorithm */

	//private boolean memThread 		= false;
	private String numThreads 			= "0";		/**< The number of threads to use with the threaded version */

	//Paired or single reads
	private boolean pairedReads 		= true;		/**< The option to use paired reads */
	private boolean singleReads 		= false;	/**< The option to use single reads */

	//Index path
	private String indexPath 			= "";		/**< The index path */

	private String outputHdfsDir		= "";		/**< The String containing the output path in HDFS */

	private String inputPath			= "";		/**< The first of the FASTQ files */
	private String inputPath2			= "";		/**< The second of the FASTQ files */

	private boolean sortFastqReads		= false;	/**< Sort the input reads using Spark or not */
	private boolean sortFastqReadsHdfs	= false;	/**< Sort the input reads using HDFS or not*/


	/**<
	 * String with the correct use of the program
	 */
	private String correctUse 			= "spark-submit --class SparkBWA [Spark options] SparkBWA.jar [SparkBWA Options] Input.fastq [Input2.fastq] Output\n"
			+ "\n\n"
			+ "To set the Input.fastq - setInputPath(string)\n"
			+ "To set the Input2.fastq - setInputPath2(string)\n"
			+ "To set the Output - setOutputPath(string)\n"
			+ "The available SparkBWA options are: \n\n";

	/**<
	 * Header to show when the program is not launched correctly
	 */
	private String header 				= "Performs genomic alignment using bwa in a Hadoop cluster\n\n";
	
	/**<
	 * Footer to show when the program is not launched correctly
	 */
	private String footer 				= "\nPlease report issues at josemanuel.abuin@usc.es";


	private String outputPath			= "";		/**< The output SAM file */

	private int partitionNumber			= 0;		/**< Number of partitions to parallelize */



	private static final Log LOG 		= LogFactory.getLog(BwaOptions.class);		/**< The Log */

	/**
	 * Constructor to use from the Spark shell. No arguments are provided
	 */
	public BwaOptions(){

		Options options = this.initOptions();

		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( correctUse,header, options,footer , true);

	}

	/**
	 * Constructor to use from within SparkBWA from the Linux console
	 * @param args The arguments that the user is going to provide from the Linux console
	 */
	public BwaOptions(String [] args){
		
		//Parse arguments
		for(String argumento: args){
			LOG.info("JMAbuin:: Received argument: "+argumento);
		}


		//Algorithm options
		Options options = this.initOptions();

		//To print the help
		HelpFormatter formatter = new HelpFormatter();
		//formatter.printHelp( correctUse,header, options,footer , true);

		//Parse the given arguments
		CommandLineParser parser = new BasicParser();
		CommandLine cmd;
		try {
			cmd = parser.parse( options, args);


			//Number of threads per map task
			if(cmd.hasOption("threads")){
				numThreads = cmd.getOptionValue("threads");
			}

			//We look for the algorithm
			if (cmd.hasOption("algorithm")) {


				if(cmd.getOptionValue("algorithm").equals("mem")){
					//Case of the mem algorithm
					memAlgorithm = true;
					alnAlgorithm = false;
					bwaswAlgorithm = false;
				}

				else if(cmd.getOptionValue("algorithm").equals("aln")) {
					// Case of aln algorithm
					alnAlgorithm = true;
					memAlgorithm = false;
					bwaswAlgorithm = false;
				}

				else if(cmd.getOptionValue("algorithm").equals("bwasw")) {
					// Case of bwasw algorithm
					bwaswAlgorithm = true;
					memAlgorithm = false;
					alnAlgorithm = false;
				}

				else{
					LOG.warn("The algorithm "+cmd.getOptionValue("algorithm")+" could not be found\nSetting to default mem algorithm\n");
					memAlgorithm = true;
					alnAlgorithm = false;
					bwaswAlgorithm = false;

				}

			}


			//We look for the index
			if(cmd.hasOption("index")){
				indexPath = cmd.getOptionValue("index");
			}
			else{
				LOG.error("No index has been found. Aborting.");
				formatter.printHelp( correctUse,header, options,footer , true);
				System.exit(1);
			}

			//Partition number
			if(cmd.hasOption("partitions")){
				partitionNumber = Integer.parseInt(cmd.getOptionValue("partitions"));
			}

			//We look if we want the paired or single algorithm
			if(cmd.hasOption("reads")) {
				if(cmd.getOptionValue("reads").equals("single")) {
					pairedReads = false;
					singleReads = true;
				}

				else if (cmd.getOptionValue("reads").equals("paired")) {
					pairedReads = true;
					singleReads = false;
				}

				else {
					LOG.warn("Reads argument could not be found\nSetting it to default paired reads\n");
					pairedReads = true;
					singleReads = false;
				}
			}

			//We look if the user wants to use a reducer or not
			if(cmd.hasOption("r")){
				useReducer = true;
			}
			else{
				useReducer = false;
			}

			//Sorting input reads
			if(cmd.hasOption("sorting")) {
				if(cmd.getOptionValue("sorting").equals("hdfs")) {
					this.sortFastqReadsHdfs = true;
					this.sortFastqReads = false;
				}
				else if (cmd.getOptionValue("sorting").equals("spark")) {
					this.sortFastqReadsHdfs = false;
					this.sortFastqReads = true;
				}
			}

			//Input and output paths
			String otherArguments[] = cmd.getArgs(); //With this we get the rest of the arguments

			if((otherArguments.length != 2) && (otherArguments.length != 3)){
				LOG.error("No input and output has been found. Aborting.");

				for(String tmpString: otherArguments){
					LOG.error("Other args:: "+tmpString);
				}

				formatter.printHelp( correctUse,header, options,footer , true);
				System.exit(1);
			}

			else if(otherArguments.length == 2){
				inputPath = otherArguments[0];
				outputPath = otherArguments[1];
			}
			else if (otherArguments.length == 3){
				inputPath = otherArguments[0];
				inputPath2 = otherArguments[1];
				outputPath = otherArguments[2];
			}


		} catch (UnrecognizedOptionException e){
			e.printStackTrace();
			formatter.printHelp( correctUse,header, options,footer , true);
			System.exit(1);

		} catch (ParseException e) {
			//formatter.printHelp( correctUse,header, options,footer , true);
			e.printStackTrace();
			System.exit(1);
		}


	}

	/**
	 * Function to init the SparkBWA available options
	 * @return An Options object containing the available options
	 */
	public Options initOptions(){


		Options options = new Options();

		//Algorithm options
		Option algorithm = new Option("algorithm",true,"Specify the algorithm to use during the alignment");
		algorithm.setArgName("mem|aln|bwasw");

		options.addOption(algorithm);

		//Number of threads
		Option threads = new Option("threads",true,"Number of threads used per map - setNumThreads(string)");
		threads.setArgName("Threads number");

		options.addOption(threads);

		//Paired or single reads
		Option reads = new Option("reads",true,"Type of reads to use during alignment");
		reads.setArgName("paired|single");

		options.addOption(reads);

		//Reducer option
		Option reducer = new Option("r",false,"Enables the reducer phase - setUseReducer(boolean)");
		options.addOption(reducer);


		//Index
		Option index = new Option("index",true,"Prefix for the index created by bwa to use - setIndexPath(string)");
		index.setArgName("Index prefix");


		options.addOption(index);

		//Partition number
		Option partitions = new Option("partitions", true, "Number of partitions to divide input reads - setPartitionNumber(int)");
		partitions.setArgName("Number of partitions");


		options.addOption(partitions);

		Option sorting = new Option("sorting",true,"Type of algorithm used to sort input FASTQ reads");
		sorting.setArgName("hdfs|spark");

		options.addOption(sorting);


		return options;
	}

	/**
	 * Setter for the output directory in HDFS
	 * @param outputHdfsDir String containing the path in HDFS where results are going to be stored
	 */
	public void setOutputHdfsDir(String outputHdfsDir) {
		this.outputHdfsDir = outputHdfsDir;
	}

	/**
	 * Getter for the output directory in HDFS
	 * @return A string containing the output directory in HDFS
	 */
	public String getOutputHdfsDir() {
		return outputHdfsDir;
	}
	
	/**
	 * Getter to know if a reducer is going to be used or not
	 * @return A boolean value that is true if a reducer is used or not
	 */
	public boolean isUseReducer() {
		return useReducer;
	}

	/**
	 * Setter for the option of using a reducer
	 * @param useReducer A boolean value that is true ir a reducer is going to be used or false otherwise
	 */
	public void setUseReducer(boolean useReducer) {
		this.useReducer = useReducer;
	}

	/**
	 * Getter for the option of the "mem" algorithm
	 * @return A boolean value that is true if the "mem" algorithm is going to be used
	 */
	public boolean isMemAlgorithm() {
		return memAlgorithm;
	}

	/**
	 * Setter for the option of the "mem" algorithm
	 * @param memAlgorithm A boolean value that is true if the "mem" algorithm is going to be used
	 */
	public void setMemAlgorithm(boolean memAlgorithm) {
		this.memAlgorithm = memAlgorithm;

		if(memAlgorithm){
			this.setAlnAlgorithm(false);
			this.setBwaswAlgorithm(false);
		}

	}

	/**
	 * Getter for the option of the "aln" algorithm
	 * @return A boolean value that is true if the "aln" algorithm is going to be used
	 */
	public boolean isAlnAlgorithm() {
		return alnAlgorithm;
	}

	/**
	 * Setter for the option of the "aln" algorithm
	 * @param alnAlgorithm A boolean value that is true if the "aln" algorithm is going to be used
	 */
	public void setAlnAlgorithm(boolean alnAlgorithm) {
		this.alnAlgorithm = alnAlgorithm;

		if(alnAlgorithm){
			this.setMemAlgorithm(false);
			this.setBwaswAlgorithm(false);
		}
	}

	/**
	 * Getter for the option of the "bwasw" algorithm
	 * @return A boolean value that is true if the "bwasw" algorithm is going to be used
	 */
	public boolean isBwaswAlgorithm() {
		return bwaswAlgorithm;
	}

	/**
	 * Setter for the option of the "bwasw" algorithm
	 * @param bwaswAlgorithm A boolean value that is true if the "bwasw" algorithm is going to be used
	 */
	public void setBwaswAlgorithm(boolean bwaswAlgorithm) {
		this.bwaswAlgorithm = bwaswAlgorithm;

		if(bwaswAlgorithm){
			this.setAlnAlgorithm(false);
			this.setMemAlgorithm(false);
		}
	}

	/**
	 * Getter for the number of threads to use with the threaded version of BWA
	 * @return The number of threads to use. If it is 0, it is sequential
	 */
	public String getNumThreads() {
		return numThreads;
	}

	/**
	 * Setter for the number of threads to use in the threaded version of BWA
	 * @param numThreads Integer that represents the number of threads to use
	 */
	public void setNumThreads(String numThreads) {
		this.numThreads = numThreads;
	}

	/**
	 * Getter to know if the paired reads are going to be used
	 * @return A boolean value that indicates if paired reads are used
	 */
	public boolean isPairedReads() {
		return pairedReads;
	}

	/**
	 * Setter for the option of paired reads
	 * @param pairedReads Boolean value that indicates if the paired reads are going to be used or not
	 */
	public void setPairedReads(boolean pairedReads) {
		this.pairedReads = pairedReads;
	}

	/**
	 * Getter to know if the single reads are going to be used
	 * @return A boolean value that indicates if single reads are used
	 */
	public boolean isSingleReads() {
		return singleReads;
	}

	/**
	 * Setter for the option of single reads
	 * @param singleReads Boolean value that indicates if the single reads are going to be used or not
	 */
	public void setSingleReads(boolean singleReads) {
		this.singleReads = singleReads;
	}

	/**
	 * Getter for the index path
	 * @return A String containing the path for the index
	 */
	public String getIndexPath() {
		return indexPath;
	}

	/**
	 * Setter for the index path
	 * @param indexPath A String containing the path where the index is stored
	 */
	public void setIndexPath(String indexPath) {
		this.indexPath = indexPath;
	}

	/**
	 * Getter for the first of the FASTQ files
	 * @return A String with the path of the first of the FASTQ files
	 */
	public String getInputPath() {
		return inputPath;
	}

	/**
	 * Setter for the first of the FASTQ files
	 * @param inputPath A String containing the path of the first of the FASTQ files
	 */
	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	/**
	 * Getter for the output path
	 * @return A String containing the location where the output files are going to be stored
	 */
	public String getOutputPath() {
		return outputPath;
	}

	/**
	 * Setter for the output path
	 * @param outputPath A String containing the location where the output files are going to be stored
	 */
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	/**
	 * Getter for the number of partitions to use
	 * @return An integer that represents the number of partitions to use
	 */
	public int getPartitionNumber() {
		return partitionNumber;
	}

	/**
	 * Setter for the number of partitions to use
	 * @param partitionNumber An integer that represents the number of partitions to use
	 */
	public void setPartitionNumber(int partitionNumber) {
		this.partitionNumber = partitionNumber;
	}

	/**
	 * Getter for the second of the FASTQ files
	 * @return A String with the path of the second of the FASTQ files
	 */
	public String getInputPath2() {
		return inputPath2;
	}

	/**
	 * Setter for the second of the FASTQ files
	 * @param inputPath2 A String with the path of the second of the FASTQ files
	 */
	public void setInputPath2(String inputPath2) {
		this.inputPath2 = inputPath2;
	}
	
	/**
	 * Getter for the option to sort the input reads using Spark
	 * @return A boolean value that represents if the sorting method using Spark is going to be used
	 */
	public boolean isSortFastqReads() {
		return sortFastqReads;
	}

	/**
	 * Setter for the option to sort the input reads using Spark
	 * @param sortFastqReads A boolean value that represents if the sorting method using Spark is going to be used
	 */
	public void setSortFastqReads(boolean sortFastqReads) {
		this.sortFastqReads = sortFastqReads;
	}

	/**
	 * Getter for the option to sort the input reads using HDFS
	 * @return A boolean value that represents if the sorting method using HDFS is going to be used
	 */
	public boolean isSortFastqReadsHdfs() {
		return sortFastqReadsHdfs;
	}

	/**
	 * Setter for the option to sort the input reads using HDFS
	 * @param sortFastqReadsHdfs A boolean value that represents if the sorting method using HDFS is going to be used
	 */
	public void setSortFastqReadsHdfs(boolean sortFastqReadsHdfs) {
		this.sortFastqReadsHdfs = sortFastqReadsHdfs;
	}
}



