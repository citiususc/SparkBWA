include ./Makefile.common

.PHONY: sparkbwa libbwa.so bwa clean

all: sparkbwa_java
	@echo "================================================================================"
	@echo "SparkBWA has been compiled."
	@echo "Location    = $(LOCATION)/$(BUILD_DIR)/"
	@echo "JAVA_HOME   = $(JAVA_HOME)"
	@echo "HADOOP_HOME = $(HADOOP_HOME)"
	@echo "================================================================================"

bwa:
	$(MAKE) -C $(BWA_DIR)/$(BWA)
	if [ ! -d "$(BUILD_DIR)" ]; then mkdir $(BUILD_DIR); fi
	cp $(BWA_DIR)/$(BWA)/*.o $(BUILD_DIR)

sparkbwa:
	if [ ! -d "$(BUILD_DIR)" ]; then mkdir $(BUILD_DIR); fi
	$(CC) $(SPARKBWA_FLAGS) $(SRC_DIR)/bwa_jni.c -o $(BUILD_DIR)/bwa_jni.o $(LIBBWA_LIBS) 

libbwa.so: sparkbwa bwa
	$(CC) $(LIBBWA_FLAGS) $(BUILD_DIR)/libbwa.so $(BUILD_DIR)/*.o $(LIBBWA_LIBS)
	cd $(BUILD_DIR) && zip -r bwa ./* && cd ..

sparkbwa_java: libbwa.so
	cd $(LIBS_DIR) && wget $(SPARK_URL) && tar xzvf $(SPARK_PACKAGE) && cp spark-1.6.1-bin-hadoop2.6/lib/spark-assembly-1.6.1-hadoop2.6.0.jar ./ && rm -Rf spark-1.6.1-bin-hadoop2.6 && rm $(SPARK_PACKAGE) && cd ..
	$(JAVAC) -cp $(JAR_FILES) -d $(BUILD_DIR) -Xlint:none $(SRC_DIR)/*.java
	cd $(BUILD_DIR) && $(JAR) cfe SparkBWA.jar SparkBWA ./*.class && cd ..
	#cd $(BUILD_DIR) && $(JAR) cfe SparkBWASeq.jar BwaSeq ./*.class && cd ..

clean:
	$(RMR) $(BUILD_DIR)
	$(MAKE) clean -C $(BWA_DIR)/$(BWA)
