
Directory Structure

1) logs - contains logs of all the programs run on aws

2) ouptuts - contains output of all the programs run on aws 
	a) combiner
	b) in_mapper
	c) no_combiner
	d) secondary_sort (run locally as mentioned by Prof. on piazza)

3) source contains src, makefile, pom for each of the following
	a) combiner 
	b) inMapperComb
	c) noCombiner
	d) secondarySort

4) Report.pdf

5) readme.txt



Program Execution

Before running any program please include the input file "1991.csv" for the 1st problem and "csv" in case of Secondary Sort program in the following folders
1) 1991.csv file has to be included into each directory i.e. combiner, inMapperComb, noCombiner 
2) all files from 1880.csv to 1889.csv must be included in a "csv" folder and include this folder into "secodarySort" folder

After going in the specific directory in your terminal eg: /home/akash/Desktop/CS6240/hw2/HW2 Submission/Source and Build Files/combiner
1) "make" command to make the build
2) "make local" command to execute 

