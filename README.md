# BigBWA
Approaching the Burrows-Wheeler Aligner to Big Data Technologies

# What's BigBWA about? #

**BigBWA** is a tool to run the Burrows-Wheeler Aligner--[BWA][1] on a [Hadoop][2] cluster. The current version of BigBWA (v0.1, march 2015) supports the following BWA algorithms:

* **BWA-MEM**
* **BWA-ALN**
* **BWA-SW**

All of them work with paired and single reads.

# Structure #
In this GitHub repository you can find the following directories:

* bwa - This folder contains the BWA software package required to build **BigBWA**. Currently it includes versions 0.5.10-mt and 0.7.12, but **BigBWA** is able to work with old or later versions of BWA.
* libs - It contains the Hadoop libraries needed to build **BigBWA**. By default, **BigBWA** uses the 64 bits libraries. User can also find the 32 bits libraries in a subfolder.
* src - **BigBWA** source code.

# Getting started #

## Requirements
Requirements to build **BigBWA** are the same than the ones to build BWA, with the only exception that the *JAVA_HOME* environment variable should be defined. If not, you can define it in the *Makefile.common* file. 

It is also needed to include the flag *-fPIC* in the *Makefile* of the considered BWA version. To do this, the user just need to add this option to the end of the *CFLAGS* variable in the BWA Makefile. Considering bwa-0.7.12, the original Makefile contains:

	CFLAGS=		-g -Wall -Wno-unused-function -O2

and after the change it should be:

	CFLAGS=		-g -Wall -Wno-unused-function -O2 -fPIC

To build the jar file required to execute on a Hadoop cluster, four hadoop jars are necessary. These jar files can be found inside the "libs" folder. Depending on the Hadoop installation, users should use libs/ (for 64 bits) or libs/libs32 (for 32 bits). This can also be configured in *Makefile.common*.

## Building
The default way to build **BigBWA** is:

	git clone https://github.com/citiususc/BigBWA.git
	cd BigBWA
	make
		
This will create the *build* folder, which will contain two main files:

* **BigBWA.jar** - Jar file to launch with Hadoop.
* **bwa.zip** - File with the BWA library needed to execute with Hadoop. It needs to be uploaded to the Hadoop distributed cache.

## Running BigBWA ##
**BigBWA** requires a working Hadoop cluster. Users should take into account that at least 9 GB of free memory per map are required (each map loads into memory the bwa index). Note that **BigBWA** uses disk space in the Hadoop tmp directory.

Here it is an example of how to run **BigBWA** with the BWA-MEM paired algorithm. This example assumes that our index is store in all the cluster nodes at */Data/HumanBase/* . The index can be obtained with BWA, using "bwa index".

First, we get the input Fastq reads from the [1000 Genomes Project][3] ftp:

	wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data/NA12750/sequence_read/ERR000589_1.filt.fastq.gz
	wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data/NA12750/sequence_read/ERR000589_2.filt.fastq.gz
	
Next, the downloaded files should be uncompressed:

	gzip -d ERR000589_1.filt.fastq.gz
	gzip -d ERR000589_2.filt.fastq.gz
	
and prepared to be used by BigBWA:

	python src/utils/Fq2FqBigDataPaired.py ERR000589_1.filt.fastq ERR000589_2.filt.fastq ERR000589.fqBD

	hdfs dfs -copyFromLocal ERR000589.fqBDP ERR000589.fqBDP
	
Finally, we can execute **BigBWA** on the Hadoop cluster:

	hadoop jar BigBWA.jar -archives bwa.zip -D mapreduce.input.fileinputformat.split.minsize=123641127 -D mapreduce.input.fileinputformat.split.maxsize=123641127 -mem -paired -index /Data/HumanBase/hg19 -r ERR000589.fqBDP ExitERR000589

Options:
* **-mem** - use the BWA-MEM algorithm.
* **-paired** - the algorithm uses paired reads.
* **-index** - the index prefix is specified. The index must be available in all the cluster nodes at the same location.
* **-r** - a reducer will be used.
* The last two arguments are the input and output in HDFS.

If you want to check all the available options, execute the command:

	hadoop jar BigBWA.jar

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
