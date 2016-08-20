.PHONY: sparkbwa libbwa.so bwa clean

CC = gcc
JAR = jar
RM = rm -f
RMRF = rm -rf

MAKE = make
LOCATION = `pwd`
SRC_DIR = jni
BUILD_DIR = jni/build
OUTPUT_DIR = sparkbwa_out
RESOURCES_DIR = src/main/resources

# JAVA variables ####### 
ifndef JAVA_HOME 
JAVA_HOME = /usr/lib/jvm/java
JAVA_HOME_INCLUDES = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux
else
JAVA_HOME_INCLUDES = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux
endif

# Bwa variables ########
BWA_DIR = lib/
BWA = bwa-0.7.15
SPARKBWA_FLAGS = -c -g -Wall -Wno-unused-function -O2 -fPIC -DHAVE_PTHREAD -DUSE_MALLOC_WRAPPERS $(JAVA_HOME_INCLUDES)
LIBBWA_FLAGS = -shared -o
LIBBWA_LIBS = -lrt -lz



all: sparkbwa_java
	@echo "================================================================================"
	@echo "SparkBWA has been compiled."
	@echo "Location    = $(LOCATION)/$(BUILD_DIR)/"
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
	cp $(BUILD_DIR)/libbwa.so $(RESOURCES_DIR)

sparkbwa_java: libbwa.so
	mvn clean package
	if [ ! -d "$(OUTPUT_DIR)" ]; then mkdir $(OUTPUT_DIR); fi
	cp target/*.jar $(OUTPUT_DIR)

clean:
	$(RM) $(BUILD_DIR)/*
	$(RMRF) target
	$(RMRF) $(OUTPUT_DIR)
	$(MAKE) clean -C $(BWA_DIR)/$(BWA)
