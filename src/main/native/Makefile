include ./Makefile.common

.PHONY: sparkbwa libbwa.so bwa clean

all: libbwa.so
	@echo "================================================================================"
	@echo "SparkBWA library has been built."
	@echo "Location    = $(LOCATION)/$(BUILD_DIR)/"
	@echo "JAVA_HOME   = $(JAVA_HOME)"
	@echo "================================================================================"

bwa:
	$(MAKE) -C $(BWA_DIR)
	if [ ! -d "$(BUILD_DIR)" ]; then mkdir $(BUILD_DIR); fi
	cp $(BWA_DIR)/*.o $(BUILD_DIR)

sparkbwa:
	if [ ! -d "$(BUILD_DIR)" ]; then mkdir $(BUILD_DIR); fi
	$(CC) $(SPARKBWA_FLAGS) bwa_jni.c -o $(BUILD_DIR)/bwa_jni.o $(LIBBWA_LIBS)

libbwa.so: sparkbwa bwa
	$(CC) $(LIBBWA_FLAGS) $(BUILD_DIR)/libbwa.so $(BUILD_DIR)/*.o $(LIBBWA_LIBS)
	cp $(BUILD_DIR)/libbwa.so ../resources/
	#cd $(BUILD_DIR) && zip -r bwa ./* && cd ..

clean:
	$(RMR) $(BUILD_DIR)
	$(MAKE) clean -C $(BWA_DIR)