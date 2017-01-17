/**
 *
 * @author AkashKadam
 */
package com.neu.mr.hw1.avgTmax;

import java.util.ArrayList; 
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Collections;

import static com.neu.mr.hw1.avgTmax.ComputeTMAX.result;

public class Sequential {
	// this arraylist is used to store all the 10 execution times
	static ArrayList<Long> executionTime = new ArrayList<Long>();
	
	// map data structure is used to store all the stationID and its
	// corresponding tmax sum and total count of the tmax value for 
	// a specific station
	static Map<String, Info> stationSumCount; 
	long startTime, endTime, sum;

	// this is the sequential version in which the input records are 
	// filtered based on the TMAX value and then put into the hash map
	public Sequential(){
		for(int a = 0; a < 10; a++){
			stationSumCount = new HashMap<String, Info>();
			startTime = System.currentTimeMillis();
			Iterator<String> itr = result.iterator();
			while(itr.hasNext()){
				if(itr.next().contains("TMAX")){
				String[] parts = itr.next().split(",");
				if(stationSumCount.get(parts[0]) == null){
					double tmax = Integer.parseInt(parts[3]);
					double count = 1;
					stationSumCount.put(parts[0], new Info(tmax, count));
	
				}
				else{
					Info in = stationSumCount.get(parts[0]);
					in.tmax += Double.parseDouble(parts[3]);
					in.count++;
					for (int i = 1; i <= 17; i++)
				           fibonacci(i);
					
				}
				}
			}
		
		
			/*Set<String> entry = stationSumCount.keySet();
			for (String key : entry){
				Info in = stationSumCount.get(key);
				System.out.println(key + ":" + (in.tmax/ in.count));
			}*/

			endTime = System.currentTimeMillis();
			executionTime.add(endTime - startTime);

		}
		Collections.sort(executionTime);
		for(long time : executionTime){
			sum += time;
		}
		
		
		System.out.println("Minimum Time: " + executionTime.get(0));
		System.out.println("Maximum Time: " + executionTime.get(9));
		System.out.println("Average Time: " + sum / 10);
	}
	
	public static long fibonacci(int n) {
		if (n == 1 || n == 2) {
			return 1;
		}
		int f1 = 1, f2 = 1, fib = 1;
		for (int i = 3; i <= n; i++) {
			fib = f1 + f2; 
			f1 = f2;
			f2 = fib;
 
		}
		return fib;
    }
}
