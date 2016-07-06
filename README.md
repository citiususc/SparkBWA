# What's SparkBWA about? #

**SparkBWA** is a tool that integrates the Burrows-Wheeler Aligner--[BWA][1] on a [Apache Spark][4] framework running on the top of [Hadoop][2]. The current version of SparkBWA (v0.1, march 2016) supports the following BWA algorithms:

* **BWA-MEM**
* **BWA-backtrack**
* **BWA-SW**

All of them work with single-reads and paired-end reads.

If you use **SparkBWA**, please cite this article:

José M. Abuin, Juan C. Pichel, Tomás F. Pena and Jorge Amigo. ["SparkBWA: Speeding Up the Alignment of High-Throughput DNA Sequencing Data"][5]. PLoS ONE 11(5), pp. 1-21, 2016.

A version for Hadoop is available [here](https://github.com/citiususc/BigBWA).

# Structure #
In this GitHub repository you can find the following directories:

* bwa - This folder contains the BWA software package required to build **SparkBWA**. Currently it includes versions 0.5.10-mt, 0.7.12 and 0.7.15, but **SparkBWA** is able to work with old or later versions of BWA.
* libs - It contains the Spark libraries needed to build **SparkBWA**. By default, libraries are downloaded at compilation time.
* src - **SparkBWA** source code.

# Getting started #

## Requirements
Requirements to build **SparkBWA** are the same than the ones to build BWA, with the only exception that the *JAVA_HOME* environment variable should be defined. If not, you can define it in the *Makefile.common* file. 

It is also needed to include the flag *-fPIC* in the *Makefile* of the considered BWA version. To do this, the user just need to add this option to the end of the *CFLAGS* variable in the BWA Makefile. Considering bwa-0.7.15, the original Makefile contains:

	CFLAGS=		-g -Wall -Wno-unused-function -O2

and after the change it should be:

	CFLAGS=		-g -Wall -Wno-unused-function -O2 -fPIC

To build the jar file required to execute on a Hadoop cluster, the Spark jar is necessary. This jar file is downloaded and can be found inside the "libs" folder. This can also be configured in *Makefile.common*.

## Building
The default way to build **SparkBWA** is:

	git clone https://github.com/citiususc/SparkBWA.git
	cd SparkBWA
	make
		
This will create the *build* folder, which will contain two main files:

* **SparkBWA.jar** - jar file to launch with Spark.
* **bwa.zip** - File containing the BWA library needed to execute with Spark.

## Configuring Spark
Spark only need to be stored in the Hadoop cluster master node. It can be downloaded as a binary or can be built from source. Either way, some parameters need to be adjusted to run **SparkBWA**. Assuming that Spark is stored at *spark_dir*, we need to modify the following file:
* **spark_dir/conf/spark-defaults.conf**. If it does not exist, copy it from *spark-defaults.conf.template*, in the same directory.

The next two lines must be included in the file:
	
	spark.executor.extraJavaOptions		-Djava.library.path=./bwa.zip
	spark.yarn.executor.memoryOverhead	8704
	
In this way, Spark executors are able to find the BWA library (first line). The second line sets the amount of off-heap memory (in megabytes) to be allocated per YARN container.

## Running SparkBWA ##
**SparkBWA** requires a working Hadoop cluster. Users should take into account that at least 10 GB of memory per map/YARN container are required (each map loads into memory the bwa index - refrence genome). Also, note that **SparkBWA** uses disk space in the */tmp* directory.

Here it is an example of how to execute **SparkBWA** using the BWA-MEM algorithm with paired-end reads. The example assumes that our index is stored in all the cluster nodes at */Data/HumanBase/* . The index can be obtained from BWA using "bwa index".

First, we get the input FASTQ reads from the [1000 Genomes Project][3] ftp:

	wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/NA12750/sequence_read/ERR000589_1.filt.fastq.gz
	wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/NA12750/sequence_read/ERR000589_2.filt.fastq.gz
	
Next, the downloaded files should be uncompressed:

	gzip -d ERR000589_1.filt.fastq.gz
	gzip -d ERR000589_2.filt.fastq.gz
	
and uploaded to HDFS:

	hdfs dfs -copyFromLocal ERR000589_1.filt.fastq ERR000589_1.filt.fastq
	hdfs dfs -copyFromLocal ERR000589_2.filt.fastq ERR000589_2.filt.fastq
	
Finally, we can execute **SparkBWA** on the cluster. Again, we assume that Spark is stored at *spark_dir*:

	spark_dir/bin/spark-submit --class SparkBWA --master yarn-client --driver-memory 1500m --executor-memory 1500m --executor-cores 1 --archives bwa.zip --verbose --num-executors 32 SparkBWA.jar -algorithm mem -reads paired -index /Data/HumanBase/hg38 -partitions 32 ERR000589_1.filt.fastq ERR000589_2.filt.fastq Output_ERR000589

Options:
* **-algorithm mem** - Sequence alignment algorithm (mem - *BWA-MEM*, aln - *BWA-backtrack*).
* **-reads paired** - Use single or paired-end reads.
* **-bwaArgs** - Can be used to pass arguments directly to BWA (ex. "-t 4" to
  specify the amount of threads to use per instance of BWA).
* **-index** - Index prefix is specified. The index must be available in all the cluster nodes at the same location.
* The last three arguments are the input and output HDFS files.

If you want to check all the available options, execute the command:

	spark_dir/bin/spark-submit --class SparkBWA SparkBWA.jar

After the execution, in order to move the output to the local filesystem use: 

	hdfs dfs -copyToLocal Output_ERR000589/* ./
	
In case of not using a reducer, the output will be split into several pieces (files). If we want to put it together we can use one of our Python utils or use "samtools merge":

	hdfs dfs -copyToLocal Output_ERR000589/* ./
	python src/utils/FullSam.py ./ ./OutputFile.sam

## Accuracy
SparkBWA should be as accurate as running BWA normally. Below are GCAT
alignment benchmarks which proves this.

**MEM**
* [Single-reads (400 bp)](http://www.bioplanet.com/gcat/reports/7771-ilmcxyuzdb/alignment/400bp-se-large-indel/sparkbwa-mem)
* [Pair-ended reads (100 bp)](http://www.bioplanet.com/gcat/reports/7770-ecfkcezhcs/alignment/100bp-pe-small-indel/sparkbwa-mem/compare-23-18)
* [Pair-ended reads (150 bp)](http://www.bioplanet.com/gcat/reports/7782-dhjurqbogc/alignment/150bp-pe-large-indel/sparkbwa-mem/compare-67-79)

**BWA-backtrack**
* [Single-reads (100 bp)](http://www.bioplanet.com/gcat/reports/7783-mzrshfceqp/alignment/100bp-se-small-indel/sparkbwa-samse/compare-26-35)
* [Pair-ended reads (250 bp)](http://www.bioplanet.com/gcat/reports/7784-rxjsfbmmmj/alignment/250bp-pe-large-indel/sparkbwa-sampe/compare-69-81)

**BWA-SW**
* [Single-reads (400 bp)](http://www.bioplanet.com/gcat/reports/7785-gdbodiqrmn/alignment/400bp-se-large-indel/sparkbwa-bwasw/compare-49-61)
* [Pair-ended reads (250 bp)](http://www.bioplanet.com/gcat/reports/7786-hteifmsqpm/alignment/250bp-pe-small-indel/sparkbwa-bwasw/compare-68-80)


##Frequently asked questions (FAQs)

1. [I can not build the tool because *jni_md.h* or *jni.h* is missing.](#building1)
2. [SparkBWA fails with message *java.lang.UnsatisfiedLinkError: no bwa in java.library.path*.](#librarypatherror)

####<a name="building1"></a>1. I can not build the tool because *jni_md.h* or *jni.h* is missing.
You need to set correctly your *JAVA_HOME* environment variable or you can set it in Makefile.common.

####<a name="librarypatherror"></a>2. SparkBWA fails with message *java.lang.UnsatisfiedLinkError: no bwa in java.library.path*.
SparkBWA uses the Hadoop distributed cache to store the shared library that gives access to bwa from Spark, *libbwa.so*. This library is passed to each executor with the *--archives* option used when launching Spark. The argument to this option is the file *bwa.zip*. By default, this zip file is uncompressed in each executor working directory under the path *working_directory/bwa.zip/libbwa.so*. So, because of that, the option in spark-defaults.conf must be **always**:

	spark.executor.extraJavaOptions		-Djava.library.path=./bwa.zip

With this, we are indicating that the executor must search in the current working directory inside the ./bwa.sip/ directory for any library. If the user sets this option to the zip file created when building SparkBWA, the execution is going to fail.

[1]: https://github.com/lh3/bwa
[2]: https://hadoop.apache.org/
[3]: http://www.1000genomes.org/
[4]: http://spark.apache.org/
[5]: http://dx.doi.org/10.1371/journal.pone.0155461
