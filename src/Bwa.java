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

import java.io.Serializable;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Class that communicates with BWA
 * @author José M. Abuín
 * @brief This class is where the actual communication with BWA is performed.
 *
 */
public class Bwa implements Serializable {

	private static final long serialVersionUID = 1L;	/**< The version ID */
	private static final Log LOG = LogFactory.getLog(Bwa.class);

	//Option to use the reduce phase
	private boolean useReducer 				= false;			/**< The option to use the reduce phase */

	//Algorithms boolean variables
	private boolean memAlgorithm 			= false;			/**< The option to use the MEM algorithm */
	private boolean alnAlgorithm 			= false;			/**< The option to use the ALN algorithm */
	private boolean bwaswAlgorithm 			= false;			/**< The option to use the BWASW algorithm */

	private String bwaArgs  				= "";				/**< The args passed directly to bwa */

	//Paired or single reads
	private boolean pairedReads 			= false;			/**< The option to use paired reads */
	private boolean singleReads 			= false;			/**< The option to use single reads */

	//Index path
	private String indexPath 				= "";				/**< The index path */

	private String inputFile 				= "";				/**< The first of the FASTQ files */
	private String inputFile2 				= "";				/**< The second of the FASTQ files */
	private String outputFile 				= "";				/**< The output SAM file */

	private String outputHdfsDir			= "";				/**< The HDFS directory where the output files are going to be stored */

	/**
	 * @author José M. Abuín
	 * @brief This constructor is used when the BWA options are already set
	 * @param useReducer		The option o use or not a reducer
	 * @param memAlgorithm		The option to use the MEM algorithm
	 * @param alnAlgorithm		The option to use the ALN algorithm
	 * @param bwaswAlgorithm	The option to use the BWASW algorithm
	 * @param pairedReads		Use single reads
	 * @param singleReads		Use paired reads
	 * @param indexPath			The index path
	 */
	public Bwa(BwaOptions options) {
		// The object parameters are configured according the options passed as arguments
		this.useReducer 		= options.isUseReducer();

		this.memAlgorithm 		= options.isMemAlgorithm();
		this.alnAlgorithm 		= options.isAlnAlgorithm();
		this.bwaswAlgorithm 	= options.isBwaswAlgorithm();

		this.bwaArgs 			= options.getBwaArgs();

		this.pairedReads 		= options.isPairedReads();
		this.singleReads 		= options.isSingleReads();

		this.indexPath 			= options.getIndexPath();
		this.outputHdfsDir 		= options.getOutputPath();
	}

	/**
	 * Getter for the second of the input files
	 * @return A String containing the second FASTQ file name
	 */
	public String getInputFile2() {
		return inputFile2;
	}

	/**
	 * Setter for the second of the input files
	 * @param inputFile2 A String containing the second FASTQ file name
	 */
	public void setInputFile2(String inputFile2) {
		this.inputFile2 = inputFile2;
	}

	/**
	 * Getter for the first of the input files
	 * @return A String containing the first FASTQ file name
	 */
	public String getInputFile() {
		return inputFile;
	}

	/**
	 * Setter for the first of the input files
	 * @param inputFile A String containing the first FASTQ file name
	 */
	public void setInputFile(String inputFile) {
		this.inputFile = inputFile;
	}

	/**
	 * Getter for the output file
	 * @return A String containing the ouput SAM file name
	 */
	public String getOutputFile() {
		return outputFile;
	}

	/**
	 * Setter for the ouput file
	 * @param outputFile A String containing the output SAM file name
	 */
	public void setOutputFile(String outputFile) {
		this.outputFile = outputFile;
	}

	/**
	 * Getter for the option of using a reducer
	 * @return A boolean value that is true if we want to use a reducer and false otherwise
	 */
	public boolean isUseReducer() {
		return useReducer;
	}

	/**
	 * Setter for the option of using a reducer
	 * @param useReducer A boolean value that is true if we want to use a reducer and false otherwise
	 */
	public void setUseReducer(boolean useReducer) {
		this.useReducer = useReducer;
	}

	/**
	 * Getter for the option of using the mem algorithm or not
	 * @return A boolean value that is true if the mem algorithm is going to be used or false otherwise
	 */
	public boolean isMemAlgorithm() {
		return memAlgorithm;
	}

	/**
	 * Setter for the option of using the mem algorithm or not
	 * @param memAlgorithm A boolean value that is true if the mem algorithm is going to be used or false otherwise
	 */
	public void setMemAlgorithm(boolean memAlgorithm) {
		this.memAlgorithm = memAlgorithm;
	}

	/**
	 * Getter for the option of using the aln algorithm or not
	 * @return A boolean value that is true if the aln algorithm is going to be used or false otherwise
	 */
	public boolean isAlnAlgorithm() {
		return alnAlgorithm;
	}

	/**
	 * Setter for the option of using the aln algorithm or not
	 * @param alnAlgorithm A boolean value that is true if the aln algorithm is going to be used or false otherwise
	 */
	public void setAlnAlgorithm(boolean alnAlgorithm) {
		this.alnAlgorithm = alnAlgorithm;
	}

	/**
	 * Getter for the option of using the bwasw algorithm or not
	 * @return A boolean value that is true if the bwasw algorithm is going to be used or false otherwise
	 */
	public boolean isBwaswAlgorithm() {
		return bwaswAlgorithm;
	}

	/**
	 * Setter for the option of using the bwasw algorithm or not
	 * @param bwaswAlgorithm A boolean value that is true if the bwasw algorithm is going to be used or false otherwise
	 */
	public void setBwaswAlgorithm(boolean bwaswAlgorithm) {
		this.bwaswAlgorithm = bwaswAlgorithm;
	}

	/**
	 * Getter for the option of using the paired reads entries or not
	 * @return A boolean value that is true if paired reads are used or false otherwise
	 */
	public boolean isPairedReads() {
		return pairedReads;
	}

	/**
	 * Setter for the option of using paired reads or not
	 * @param pairedReads A boolean value that is true if the paired reads version is going to be used or false otherwise
	 */
	public void setPairedReads(boolean pairedReads) {
		this.pairedReads = pairedReads;
	}

	/**
	 * Getter for the option of using the single reads entries or not
	 * @return A boolean value that is true if single reads are used or false otherwise
	 */
	public boolean isSingleReads() {
		return singleReads;
	}

	/**
	 * Setter for the option of using single reads or not
	 * @param singleReads A boolean value that is true if the single reads version is going to be used or false otherwise
	 */
	public void setSingleReads(boolean singleReads) {
		this.singleReads = singleReads;
	}

	/**
	 * Getter for the index path
	 * @return A String containing the index path
	 */
	public String getIndexPath() {
		return indexPath;
	}

	/**
	 * Setter for the index path
	 * @param indexPath A String that indicates the location of the index path
	 */
	public void setIndexPath(String indexPath) {
		this.indexPath = indexPath;
	}

	/**
	 * Getter for the output HDFS path
	 * @return A String containing the output HDFS path
	 */
	public String getOutputHdfsDir() {
		return outputHdfsDir;
	}

	/**
	 * Setter for the output HDFS path
	 * @param outputHdfsDir A String that indicates the output location for the SAM files in HDFS
	 */
	public void setOutputHdfsDir(String outputHdfsDir) {
		this.outputHdfsDir = outputHdfsDir;
	}

	/**
	 * This Function is responsible for creating the options that are going to be passed to BWA
	 * @param alnStep An integer that indicates at with phase of the aln step the program is
	 * @return A Strings array containing the options which BWA was launched
	 */
	public int run(int alnStep) {
		String[] parametersArray;

		ArrayList<String> parameters = new ArrayList<String>();

		//The first parameter is always "bwa"======================================================
		parameters.add("bwa");

		//The second parameter is the algorithm election===========================================
		String algorithm = "";

		//Case of "mem" algorithm
		if(this.memAlgorithm && !this.alnAlgorithm && !this.bwaswAlgorithm){
			algorithm = "mem";
		}
		//Case of "aln" algorithm
		else if(!this.memAlgorithm && this.alnAlgorithm && !this.bwaswAlgorithm){
			//Aln algorithm and paired reads
			if (this.pairedReads) {
				//In the two first steps, the aln option is used
				if (alnStep < 2){
					algorithm = "aln";
				}
				//In the third one, the "sampe" has to be performed
				else{
					algorithm = "sampe";
				}
			}
			//Aln algorithm single reads
			else if(this.singleReads){
				//In the first step the "aln" ins performed
				if (alnStep == 0){
					algorithm = "aln";
				}
				//In the second step, the "samse" is performed
				else{
					algorithm = "samse";
				}
			}

		}
		//The last case is the "bwasw"
		else if(!this.memAlgorithm && !this.alnAlgorithm && this.bwaswAlgorithm){
			algorithm = "bwasw";
		}

		parameters.add(algorithm);

		//The third parameter is the output file===================================================
		parameters.add("-f");

		//If the algorithm is "mem", "sampe" or "samse", the output is a .sam file
		if(algorithm.equals("mem") || algorithm.equals("sampe") || algorithm.equals("samse")){
			parameters.add(this.outputFile);
		}
		//If is "aln", the output is a .sai file
		else if(algorithm.equals("aln")){
			if(alnStep == 0){
				parameters.add(this.inputFile+".sai");
			}
			else if(alnStep == 1 && this.pairedReads){
				parameters.add(this.inputFile2+".sai");
			}
		}

		if (this.bwaArgs != "") {
		    parameters.add(this.bwaArgs);
        }

		//The fifth, the index path===============================================================
		parameters.add(this.indexPath);

		//The sixth, the input files===============================================================

		//If the "mem" algorithm, we add the FASTQ files
		if(algorithm.compareTo("mem")==0){
			parameters.add(this.inputFile);

			if(this.pairedReads){
				parameters.add(this.inputFile2);
			}
		}

		//If "aln" aln step is 0 or 1, also FASTQ files
		else if(algorithm.compareTo("aln")==0){
			if(alnStep == 0){
				parameters.add(this.inputFile);
			}
			else if(alnStep == 1 && this.pairedReads){
				parameters.add(this.inputFile2);
			}
		}

		//If "sampe" the input files are the .sai from previous steps
		else if (algorithm.equals("sampe")){
			parameters.add(this.inputFile+".sai");
			parameters.add(this.inputFile2+".sai");
			parameters.add(this.inputFile);
			parameters.add(this.inputFile2);
		}

		//If "samse", only one .sai file
		else if (algorithm.equals("samse")){
			parameters.add(this.inputFile+".sai");
			parameters.add(this.inputFile);
		}

		//Initialization of the array with the previous parameters
		parametersArray = new String[parameters.size()];

		for(int i = 0; i< parameters.size(); i++){
			parametersArray[i] = parameters.get(i);
		}

		// Call to JNI with the selected parameters
		int returnCode = BwaJni.Bwa_Jni(parametersArray);

		if (returnCode != 0) {
		    LOG.error("BWA:: BWA exited with error code: " + String.valueOf(returnCode));
		    return returnCode;
        }

		// The run was successful
		return 0;
	}
}
