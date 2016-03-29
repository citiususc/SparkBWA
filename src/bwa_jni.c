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

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include "BwaJni.h"

int main(int argc,char *argv[]);


JNIEXPORT jint JNICALL Java_BwaJni_bwa_1jni (JNIEnv *env, jobject thisObj, jint argc, jobjectArray stringArray, jintArray lenStrings){

	//Parte argumentos
	char **argv;
	char **argvTmp;
	
	int stringCount = (*env)->GetArrayLength(env,stringArray);//env->GetArrayLength(stringArray);

	argvTmp = (char **) malloc(stringCount*sizeof(char **));


	int i = 0;
	int mem = 0;
	int redirect = 0;
	int getFilename = 0;
	
	char *output;
	char *algorithm;

	int numArgs = 0;

	//PreParse
	for (i=0; i<stringCount; i++) {
    	
    		//To get the current argument from Java
        	jstring string = (jstring) (*env)->GetObjectArrayElement(env, stringArray, i);
        	argvTmp[i] = (*env)->GetStringUTFChars(env, string, 0);
        
        	//We save the algorithm name
        	if(i == 1){
        		//strcpy(algorithm,argvTmp[i]);
        		algorithm = argvTmp[i];
        		fprintf(stderr, "[%s] Algorithm found %d '%s'\n",__func__,i, algorithm);
        	}
        
		//If it is the mem algorithm and the -f option has been set in the previous argument, we get the output file name.
		if(getFilename == 1){
			//strcpy(output,argvTmp[i]);
			output = argvTmp[i];
			getFilename = 0;
			fprintf(stderr, "[%s] Filename found %d '%s'\n",__func__,i, output);
		}
		
		//We set the option to get the file name in the next iteration
		//if((strcmp(argvTmp[i],"-f")==0) && (i<(stringCount-1)) && (strcmp(algorithm,"mem")==0)){
		if((strcmp(argvTmp[i],"-f")==0) && (i<(stringCount-1))){
			getFilename = 1;
			redirect = 1;
			if(strcmp(algorithm,"mem")==0){
				mem = 1;
			}
			fprintf(stderr, "[%s] Filename parameter -f found %d '%s'\n",__func__,i, argvTmp[i]);
		}
        
		fprintf(stderr, "[%s] Arg %d '%s'\n",__func__,i, argvTmp[i]);
        
	}

    
    //If it is the mem algorithm we create the new arguments
	if((mem == 1) && (redirect == 1)){
	
		argv = (char **) malloc((argc-2)*sizeof(char **));

		int j = 0;
		i = 0;
		
		while(i<stringCount){
			if(strcmp(argvTmp[i],"-f")==0){
				i++; //To skip this option and the file name
			}
			else{
				//strcpy(argv[j],argvTmp[i]);
				argv[j] = argvTmp[i];
				fprintf(stderr, "[%s] option[%d]: %s.\n", __func__,j,argv[j]);
				j++;
			}
			
			i++;
		}
		
		numArgs = argc - 2;
	}
	//If not, arguments are already set.
	else{
		argv = argvTmp;
		numArgs = argc;
	}


	//End arguments
	
	//Call to bwa main
	

	int temp_stdout;
	temp_stdout = dup(fileno(stdout));
	
	if(temp_stdout == -1){
		fprintf(stderr, "[%s] Error saving stdout.\n", __func__);
	}
	
	if(redirect == 1){
		if (freopen(output, "wb", stdout) == 0) {
			fprintf(stderr, "Fail to open file '%s' with freopen.", output);
		}
	}
	
	int ret = main(numArgs,argv);
	fprintf(stderr, "[%s] Return code from BWA %d.\n", __func__,ret);
	
	//if((strcmp(algorithm,"mem")!=0)&&(strcmp(algorithm,"sampe")!=0)){
	if(redirect == 1){
		fflush(stdout);
		fclose(stdout);
		
		FILE *fp2 = fdopen(temp_stdout, "w");
		
		stdout = fp2;
	}
	
	/*
	for (i=0; i<stringCount; i++) {
    	
        (*env)->ReleaseStringUTFChars(env, (jstring) (*env)->GetObjectArrayElement(env, stringArray, i), argv[i]);
        
    }*/
	return ret;

}


