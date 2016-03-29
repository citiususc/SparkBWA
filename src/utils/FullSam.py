#!/usr/bin/python
# -*- coding: utf-8 -*-

#Copyright 2015 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
#
#This file is part of BigBWA.
#
#BigBWA is free software: you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation, either version 3 of the License, or
#(at your option) any later version.
#
#BigBWA is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#GNU General Public License for more details.
#
#You should have received a copy of the GNU General Public License
#along with BigBWA. If not, see <http://www.gnu.org/licenses/>.

import sys
import os
import re

import subprocess

#This program takes as input a directory with sam files in it to put all alignments together in one file.
#Files must be named Name0.sam, Name1.sam ... NameN.sam to have a correct and ordered output.

def main():

	if(len(sys.argv)<3):
		print "Error!\n"
		print "Use: python FullSam.py Directory/Sam/Files/ OutputFile.sam"
		return 0


	else: 
		nomeFicheirosSam = str(sys.argv[1])
		nomeFicheiroSaida = str(sys.argv[2])


		samFiles = []

		for ficheiro in os.listdir(nomeFicheirosSam):
			if ficheiro.endswith(".sam"):
				samFiles.append(ficheiro)

		
		#print saiFiles[0]
		#print nomeSaiFiles

		expresionSam = re.compile(r'([a-zA-Z]+)[\d]+[\.]{1}[\w]+').match(samFiles[0])
		nomeSamFiles = expresionSam.group(1)
		
		ficheiroSaida = open(nomeFicheiroSaida,'w')

		readingHeader = True

		numberOfHeaderLines = 0

		for i in range(0,len(samFiles)):
			#print nomeSaiFiles+str(i)+".sai "+nomeFqFiles+str(i)+".fq"
			
			currentLine = 0
			ficheiroActual = nomeFicheirosSam+nomeSamFiles+str(i)+".sam"
			
			ficheiroEntrada = open(ficheiroActual,'r')

			for line in ficheiroEntrada:
			
				currentLine = currentLine + 1
			
				if(readingHeader and line.startswith("@")):
					ficheiroSaida.write(line)
					numberOfHeaderLines = numberOfHeaderLines + 1
				
				elif(currentLine > numberOfHeaderLines):
					readingHeader = False
					ficheiroSaida.write(line)

			ficheiroEntrada.close()


		ficheiroSaida.close()

		return 1

	
			

if __name__ == '__main__':
	main()
