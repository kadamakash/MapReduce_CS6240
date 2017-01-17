/**
 *
 * @author AkashKadam
 */
package com.neu.mr.hw1.avgTmax;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ComputeTMAX {
	// result array list is used to store all the lines 
	// read from the file
	static List<String> result = new ArrayList<String>();
	
	// IO routine to read the file and store all lines in the array list
	public static void ioRoutine(String path) throws IOException{
		/*"/home/akash/Desktop/CS6240/Homework1/1921.csv"*/
		FileReader f = new FileReader(path);
		BufferedReader br = new BufferedReader(f);
		String line;
		while((line = br.readLine()) != null){
				result.add(line);
		}
		br.close();
	}

	// main method which calls the IORoutine and the other programs
	public static void main(String args[]) throws IOException, InterruptedException{
		String path = args[0];
		System.out.println("Path: "+path);
		ioRoutine(path);
		new Sequential();
		System.out.println();
		System.out.println("Time for Sequential Execution with delay");
		System.out.println();
        System.out.println("######################################");
        System.out.println();
		new NoLock();
		System.out.println();
		System.out.println("Time for NoLock Execution with delay");
		System.out.println();
        System.out.println("######################################");
        System.out.println();
		new CoarseLock();
		System.out.println();
		System.out.println("Time for CoarseLock Execution with delay");
		System.out.println();
        System.out.println("######################################");
        System.out.println();
		new FineLock();
		System.out.println();
        System.out.println("Time for FineLock Execution with delay");
		System.out.println();
        System.out.println("######################################");
        System.out.println();
		new NoSharing();
		System.out.println();
		System.out.println("Time for NoSharing Execution with delay");
	}
}

// Info class is used to store the tmax sum and count values for each station
// in the input records
class Info {
	double tmax = 0;
	double count = 0;
	
	public Info(Double tmax, Double count){
		this.tmax = tmax;
		this.count = count;
	}

}