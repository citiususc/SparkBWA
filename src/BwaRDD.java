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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * Class that represents the FASTQ reads into a RDD
 * @brief This class contains the RDD with the paired reads. Also implements a series of functions that allow the user to work with the data stored in the RDD
 * @author José M. Abuín
 * 
 */
public class BwaRDD extends JavaRDD<Tuple2<String,String>> {

	private static final long serialVersionUID = 1L; 							/**< Version ID */

	
	private static final Log LOG = LogFactory.getLog(BwaInterpreter.class); 	/**< The LOG */

	private Bwa bwaObject; 														/**< The Bwa object. Needed to call BWA */

	private BwaOptions options;													/**< The options set to call BWA */

	/**
	 * @author José M. Abuín
	 * @param rdd The RDD<Tuple2<String,String>> that contains the FASTQ paired reads to be processed by BWA
	 * @param classTag The class tag corresponding to the RDD<Tuple2<String,String>>
	 */
	public BwaRDD(RDD<Tuple2<String,String>> rdd, ClassTag<Tuple2<String,String>> classTag) {
		super(rdd, classTag);
		
		//We try to execute the garbage collector at the begin to try to free some memory.
		System.gc();
		System.runFinalization();
		
	}

	/**
	 * Method to set the BWA options
	 * @author José M. Abuín
	 * @brief With this method it is possible to specify the options that BWA is going to run with.
	 * @param options The BWaOptions object that contains all the available options
	 */
	public void setOptions(BwaOptions options){
		
		this.options = options;

		//Creation of the Bwa object with the specified options
		Bwa bwa = new Bwa(this.options.isUseReducer(), this.options.isMemAlgorithm(), this.options.isAlnAlgorithm(), this.options.isBwaswAlgorithm(), 
						this.options.getNumThreads(), this.options.isPairedReads(), this.options.isSingleReads(), this.options.getIndexPath());
		
		//In the Bwa object, the output dir is set
		bwa.setOutputHdfsDir(options.getOutputPath());

		this.bwaObject = bwa;

	}


	/**
	 * Procedure to perform the alignment
	 * @author José M. Abuín
	 */
	public void MapBwa(){

		Configuration conf = new Configuration();
		
		//The mapPartitionsWithIndex is used over this RDD to perform the alignment. The resulting sam filenames are returned
		List<String> returnedValues = this.mapPartitionsWithIndex(new BwaAlignment(this.context(),this.bwaObject),true).collect();
		
		LOG.info("BwaRDD :: Total of returned lines from RDDs :: "+returnedValues.size());

		//In the case of use a reducer the final output has to be stored in just one file
		if(this.bwaObject.isUseReducer()){
			try {
				FileSystem fs = FileSystem.get(conf);

				Path finalHdfsOutputFile = new Path(this.bwaObject.getOutputHdfsDir()+"/FullOutput.sam");

				FSDataOutputStream outputFinalStream = fs.create(finalHdfsOutputFile, true);

				//We iterate over the resulting files in HDFS and agregate them into only one file.
				for (int i = 0; i<returnedValues.size(); i++){

					LOG.info("JMAbuin:: SparkBWA :: Returned file ::"+returnedValues.get(i));
					BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(returnedValues.get(i)))));

					String line;
					line=br.readLine();

					while (line != null){
						if(i==0 || !line.startsWith("@") ){
							
							//outputFinalStream.writeBytes(line+"\n");
							outputFinalStream.write((line+"\n").getBytes());
							
						}


						line=br.readLine();
					}

					br.close();

					fs.delete(new Path(returnedValues.get(i)), true);
				}

				outputFinalStream.close();
				//brW.close();

				fs.close();


			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				LOG.error(e.toString());
			}
		}
		else{
			for(String outputFile: returnedValues){
				LOG.info("JMAbuin:: SparkBWA :: Returned file ::"+outputFile);
			}
		}


	}

	/**
	 * Class to perform the alignment over a split from the RDD
	 * @author José M. Abuín
	 * @return A RDD containing the resulting Sam files from the alignment.
	 */
	public static class BwaAlignment implements Function2<Integer,Iterator<Tuple2<String,String>>,Iterator<String>>{

		private static final long serialVersionUID 	= 1L;	/**< Version ID */
		private String appName 						= "";	/**< The application name */
		private String appId 						= "";	/**< The application ID */
		private String tmpDir 						= "";	/**< Temp dir to store tmp files */

		private Bwa bwaInterpreter;							/**< Bwa object used in each one of the mappers */


		/**
		 * Constructor for this class
		 * @brief This constructor creates a BwaAlignment object to process in each one of the mappers
		 * @param context the SparkContext to use
		 * @param bwaInterpreter the Bwa object used to perform the alignment
		 */
		public BwaAlignment(SparkContext context, Bwa bwaInterpreter){

			this.appId 			= context.applicationId();
			this.appName 		= context.appName();
			this.tmpDir			= context.getLocalProperty("spark.local.dir");
			this.bwaInterpreter = bwaInterpreter;			

			//We set the tmp dir
			if(this.tmpDir == null || this.tmpDir == "null"){
				this.tmpDir = context.hadoopConfiguration().get("hadoop.tmp.dir");
			}
			
			if(this.tmpDir == null || this.tmpDir == "null"){
				this.tmpDir = "/tmp/";
				//this.tmpDir = context.hadoopConfiguration().get("hadoop.tmp.dir");
			}

			LOG.info("JMAbuin:: "+this.appId+" - "+this.appName);

		}

		/**
		 * Code to run in each one of the mappers. This is, the alignment with the corresponding entry data
		 * The entry data has to be written into the local filesystem
		 */
		@Override
		public Iterator<String> call(Integer arg0, Iterator<Tuple2<String,String>> arg1)
				throws Exception {

	
			Configuration conf = new Configuration();
			ArrayList<String> returnedValues = new ArrayList<String>();


			
			//STEP 1: Input fastq reads tmp file creation ====================================================================================================================
			String fastqFileName1 = this.tmpDir+this.appId+"-RDD"+arg0+"_1";
			String fastqFileName2 = this.tmpDir+this.appId+"-RDD"+arg0+"_2";

			LOG.info("JMAbuin:: Writing file: "+fastqFileName1);
			LOG.info("JMAbuin:: Writing file: "+fastqFileName2);

			File FastqFile1 = new File(fastqFileName1);
			File FastqFile2 = new File(fastqFileName2);

			FileOutputStream fos1;
			FileOutputStream fos2;
			//BufferedReader tmpSamFileBR = null;

			BufferedWriter bw1;
			BufferedWriter bw2;

			//We write the data contained in this split into the two tmp files
			try {
				fos1 = new FileOutputStream(FastqFile1);
				fos2 = new FileOutputStream(FastqFile2);

				bw1 = new BufferedWriter(new OutputStreamWriter(fos1));
				bw2 = new BufferedWriter(new OutputStreamWriter(fos2));

				Tuple2<String,String> newFastqRead;

				while(arg1.hasNext()){
					newFastqRead = arg1.next();

					bw1.write(newFastqRead._1.toString());
					bw1.newLine();

					bw2.write(newFastqRead._2.toString());
					bw2.newLine();
				}

				bw1.close();
				bw2.close();

				//We do not need the input data anymore, as it is written in a local file
				arg1 = null;
			
				
				//STEP 2: Now the options are parsed and BWA is launched by using JNI=========================================================================================
				
				//First, the two input FASYQ files are set
				bwaInterpreter.setInputFile(fastqFileName1);
				bwaInterpreter.setInputFile2(fastqFileName2);

				//The full output
				String outputSamFullFile = this.tmpDir+this.appName+"-"+this.appId+"-"+arg0+".sam";
				
				//The output filename (without the tmp directory)
				String outputSamFileName = this.appName+"-"+this.appId+"-"+arg0+".sam";

				bwaInterpreter.setOutputFile(outputSamFullFile);

				//We run BWA with the corresponding options set
				this.bwaInterpreter.run(0);

				//In case of the ALN algorithm, more executions of BWA are needed
				if(this.bwaInterpreter.isAlnAlgorithm()){

					//The next execution of BWA in the case of ALN algorithm
					this.bwaInterpreter.run(1);

					//Finally, if we are talking about paired reads and aln algorithm, a final execution is needed
					if(this.bwaInterpreter.isPairedReads()){
						this.bwaInterpreter.run(2);

						//Delete .sai file number 2
						File tmpSaiFile2 = new File(fastqFileName2+".sai");
						tmpSaiFile2.delete();
					}

					//Delete *.sai file number 1
					File tmpSaiFile1 = new File(fastqFileName1+".sai");
					tmpSaiFile1.delete();

				}



				//STEP 3: Copy result to HDFS and delete temporary files =====================================================================================================
				String outputDir = this.bwaInterpreter.getOutputHdfsDir();

				FastqFile1.delete();
				FastqFile2.delete();


				FileSystem fs = FileSystem.get(conf);

				fs.copyFromLocalFile(new Path(outputSamFullFile), new Path(outputDir+"/"+outputSamFileName));


				File tmpSamFullFile = new File(outputSamFullFile);
				tmpSamFullFile.delete();

				returnedValues.add(outputDir+"/"+outputSamFileName);

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				LOG.error(e.toString());
			}

			return returnedValues.iterator();
		}

	}



}
