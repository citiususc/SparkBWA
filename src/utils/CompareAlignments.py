#!/usr/bin/python
# -*- coding: utf-8 -*-

#Copyright 2016 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
#
#This file is part of SparkGenomics.
#
#SparkGenomics is free software: you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation, either version 3 of the License, or
#(at your option) any later version.
#
#SparkGenomics is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#GNU General Public License for more details.
#
#You should have received a copy of the GNU General Public License
#along with SparkGenomics. If not, see <http://www.gnu.org/licenses/>.

import sys
import os
import re

import subprocess

#This program takes as input two sam files and produces the differences between the first eleven fields of the reads

#GLOBAL VARIABLES
chunkLines = 100000

def compareItems(item1, item2):
	identical = True

	for i in xrange(0,4):
		if(item1[i] != item2[i]):
			identical = False
	
	for i in xrange(5,11):
		if(item1[i] != item2[i]):
			identical = False
	
	return identical

def main():

	if(len(sys.argv)<3):
		print "Error!\n"
		print "Use: python CompareAlignments.py File1.sam File2.sam OutputFile"
		return 0


	else:
		
		dataFile1 = {}
		dataFile2 = {}
		
		nomeFicheiroSam1 = str(sys.argv[1])
		nomeFicheiroSam2 = str(sys.argv[2])
		
		nomeFicheiroSaida = str(sys.argv[3])


		
		ficheiroSam1 = open(nomeFicheiroSam1,'r')
		ficheiroSam2 = open(nomeFicheiroSam2,'r')

		ficheiroSaida = open(nomeFicheiroSaida,'w');

		readingHeader = True

		numberOfLines = 0
		totalLines = 0
		
		#First read and skip header
		while readingHeader:
			line = ficheiroSam1.readline()
			line2 = ficheiroSam2.readline()
			
			if(not line.startswith("@")):
				readingHeader = False



		iterationNumber = 0
		#for line in ficheiroSam1:
		while line:
			

			#Fills data
			while ( (line or line2) and numberOfLines<chunkLines):
			
				if(line):
					fieldsLine1 = line.split("\t")
					
					dataFile1[fieldsLine1[0]+"-"+fieldsLine1[1]] = fieldsLine1

					line = ficheiroSam1.readline()			


				if(line2):
								
					fieldsLine2 = line2.split("\t")
			
				
					dataFile2[fieldsLine2[0]+"-"+fieldsLine2[1]] = fieldsLine2
				
					line2 = ficheiroSam2.readline()
				
				numberOfLines += 1

			totalLines += numberOfLines
			
			#Compare data from chunk
			for key in dataFile1.keys():

				if( dataFile2.has_key(key) and compareItems(dataFile1[key],dataFile2[key])):
				
					del dataFile1[key]
					del dataFile2[key]
			
			iterationNumber += 1
			
			print "Compared "+str(totalLines)+" lines."
			numberOfLines = 0


		ficheiroSaida.write("==========First file different fields==========\n")

		numberOfRecordsDifferent = 0
		numberOfRecordsInFile1 = 0
		numberOfRecordsInFile2 = 0

		for key in dataFile1.keys():
			lineToWrite = ""
		
			for i in xrange(0,len(dataFile1[key])):
				if(i!=len(dataFile1[key])-1):
					lineToWrite += dataFile1[key][i]+"\t"
				else:
					lineToWrite += dataFile1[key][i]
					
			ficheiroSaida.write("<--- "+lineToWrite)
			
			if(dataFile2.has_key(key)):
				lineToWrite = ""
		
				for i in xrange(0,len(dataFile2[key])):
					if(i!=len(dataFile2[key])-1):
						lineToWrite += dataFile2[key][i]+"\t"
					else:
						lineToWrite += dataFile2[key][i]
					
				ficheiroSaida.write("---> "+lineToWrite)
				
				numberOfRecordsDifferent += 1
			else:
				numberOfRecordsInFile1 += 1
				
			
		
		ficheiroSaida.write("==========Second file different fields==========\n")
		
		for key in dataFile2.keys():
			lineToWrite = ""
		
			if(not dataFile1.has_key(key)):
			
				numberOfRecordsInFile2 += 1
			
				for i in xrange(0,len(dataFile2[key])):
					if(i!=len(dataFile2[key])-1):
						lineToWrite += dataFile2[key][i]+"\t"
					else:
						lineToWrite += dataFile2[key][i]
					
				ficheiroSaida.write("---> "+lineToWrite)
			

				
		ficheiroSam1.close()
		ficheiroSam2.close()

		ficheiroSaida.close()

		print "================= RESUME ================="
		print "Total number of records: "+str(totalLines)
		print "Different records: "+str(numberOfRecordsDifferent)
		print "Records in "+nomeFicheiroSam1+" but not in "+nomeFicheiroSam2+": "+str(numberOfRecordsInFile1)
		print "Records in "+nomeFicheiroSam2+" but not in "+nomeFicheiroSam1+": "+str(numberOfRecordsInFile2)
		print "Percentage of different records: "+str((float(numberOfRecordsDifferent)/float(totalLines))*100.0)
		print "Percentage of records in "+nomeFicheiroSam1+" but not in "+nomeFicheiroSam2+": "+str((float(numberOfRecordsInFile1)/float(totalLines))*100.0)
		print "Percentage of records in "+nomeFicheiroSam2+" but not in "+nomeFicheiroSam1+": "+str((float(numberOfRecordsInFile2)/float(totalLines))*100.0)


		return 1

	
			

if __name__ == '__main__':
	main()
