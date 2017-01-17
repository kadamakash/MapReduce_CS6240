
Directory Structure

1) logs - contains logs of all the programs run locally and on aws

2) ouptuts - contains output of all the programs run locally and on aws 

3) source contains src, makefile, pom for the program

4) Report.pdf

5) readme.txt



Program Execution

Before running any program please include the input file "wikipedia-simple-html" in the "input" directory

Note : Please update the following input/output paths in the specific map-reduce jobs in the PageRank.java file acoording to your directory structure

if(i==0){
	 in = "/home/akash/Desktop/CS6240/hw3/HW3Submission/source/output"; // update path before "/output"
	}
	else 
		in = "/home/akash/Desktop/CS6240/hw3/HW3Submission/source/PageRank-" + (i-1); // update path before "/PageRank-" + (i-1);"

String in1 = "/home/akash/Desktop/CS6240/hw3/HW3Submission/source/PageRank-10"; //update path before "PageRank-10"

String out1 = "/home/akash/Desktop/CS6240/hw3/HW3Submission/source/job-3-output"; // update path before "/job-3-output"


After going in the specific directory in your terminal for eg: /home/akash/Desktop/CS6240/hw3/HW3Submission/source 
1) "make" command to make the build
2) "make local" command to execute 

