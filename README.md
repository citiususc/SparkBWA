# SparkBWA
Speeding Up the Alignment of High-Throughput DNA Sequencing Data

# What's SparkBWA about? #

**SparkBWA** is a tool to run the Burrows-Wheeler Aligner--[BWA][1] on a [Spark][4] cluster running [Hadoop][2] cluster. The current version of SparkBWA (v0.1, march 2016) supports the following BWA algorithms:

* **BWA-MEM**
* **BWA-ALN**

All of them work with paired reads.

# Structure #
In this GitHub repository you can find the following directories:

* bwa - This folder contains the BWA software package required to build **SparkBWA**. Currently it includes versions 0.5.10-mt and 0.7.12, but **SparkBWA** is able to work with old or later versions of BWA.
* libs - It contains the Spark libraries needed to build **SparkBWA**. By default, the Spark libraries are downloaded at compilation time.
* src - **SparkBWA** source code.

# Getting started #

## Requirements
Requirements to build **SparkBWA** are the same than the ones to build BWA, with the only exception that the *JAVA_HOME* environment variable should be defined. If not, you can define it in the *Makefile.common* file. 

It is also needed to include the flag *-fPIC* in the *Makefile* of the considered BWA version. To do this, the user just need to add this option to the end of the *CFLAGS* variable in the BWA Makefile. Considering bwa-0.7.12, the original Makefile contains:

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

* **SparkBWA.jar** - Jar file to launch with Spark.
* **bwa.zip** - File with the BWA library needed to execute with Spark.

## Running SparkBWA ##
**SparkBWA** requires a working Hadoop cluster. Users should take into account that at least 9 GB of free memory per map are required (each map loads into memory the bwa index). Note that **SparkBWA** uses disk space in the Hadoop tmp directory.

Here it is an example of how to run **SparkBWA** with the BWA-MEM paired algorithm. This example assumes that our index is store in all the cluster nodes at */Data/HumanBase/* . The index can be obtained with BWA, using "bwa index".

First, we get the input Fastq reads from the [1000 Genomes Project][3] ftp:

	wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data/NA12750/sequence_read/ERR000589_1.filt.fastq.gz
	wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data/NA12750/sequence_read/ERR000589_2.filt.fastq.gz
	
Next, the downloaded files should be uncompressed:

	gzip -d ERR000589_1.filt.fastq.gz
	gzip -d ERR000589_2.filt.fastq.gz
	
and uploaded to HDFS:

	hdfs dfs -copyFromLocal ERR000589_1.filt.fastq ERR000589_1.filt.fastq
	hdfs dfs -copyFromLocal ERR000589_2.filt.fastq ERR000589_2.filt.fastq
	
Finally, we can execute **SparkBWA** on the cluster:

	hadoop jar SparkBWA.jar -archives bwa.zip -D mapreduce.input.fileinputformat.split.minsize=123641127 -D mapreduce.input.fileinputformat.split.maxsize=123641127 -mem -paired -index /Data/HumanBase/hg19 -r ERR000589.fqBDP ExitERR000589

Options:
* **-mem** - use the BWA-MEM algorithm.
* **-paired** - the algorithm uses paired reads.
* **-index** - the index prefix is specified. The index must be available in all the cluster nodes at the same location.
* **-r** - a reducer will be used.
* The last two arguments are the input and output in HDFS.

If you want to check all the available options, execute the command:

	hadoop jar SparkBWA.jar

After the execution, to move the output to the local filesystem use: 

	hdfs dfs -copyToLocal ExitERR000589/part-r-00000 ./
	
In case of not using a reducer, the output will be splited into several pieces. If we want to put it together we can use one of our Python utils or use "samtools merge":

	hdfs dfs -copyToLocal ExitERR000589/Output* ./
	python src/utils/FullSam.py ./ ./OutputFile.sam
	
##Frequently asked questions (FAQs)

1. [I can not build the tool because *jni_md.h* or *jni.h* is missing.](#building1)

####<a name="building1"></a>1. I can not build the tool because *jni_md.h* or *jni.h* is missing.
You need to set correctly your *JAVA_HOME* environment variable or you can set it in Makefile.common.

[1]: https://github.com/lh3/bwa
[2]: https://hadoop.apache.org/
[3]: http://www.1000genomes.org/
[4]: http://spark.apache.org/
