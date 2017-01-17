/**
 *
 * @author AkashKadam
 */
package com.neu.mr.hw1.avgTmax;

import static com.neu.mr.hw1.avgTmax.ComputeTMAX.result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CoarseLock {
	
	static ArrayList<Long> executionTime = new ArrayList<Long>();
	static Map<String, Info> stationSumCount;
	long sum;
	
	public CoarseLock() throws InterruptedException{
		for(int a = 0; a < 10; a++){
			stationSumCount = new HashMap<String, Info>();
			long startTime = System.currentTimeMillis();
			
			CoarseLockAvg t1 = new CoarseLockAvg(0, result.size()/2);
			CoarseLockAvg t2 = new CoarseLockAvg(result.size()/2, result.size());
			t1.start(); t2.start();
			t1.join(); t2.join();
			
			/*Set<String> entry = stationSumCount.keySet();
			for (String key : entry){
				Info in = stationSumCount.get(key);
				System.out.println(key + ":" + (in.tmax/ in.count));
			}*/
			
			long endTime = System.currentTimeMillis();
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

	// synchronized the method so a thread has to obtain a lock before 
	// it manipulates the data structure, so it is obtaining a lock on the single data structure
	public static synchronized void computeAvgTmax(int index){
		String line = result.get(index);
		if(line.contains("TMAX")){
			String[] parts = line.split(",");
			if(stationSumCount.get(parts[0]) == null){
				double tmax = Integer.parseInt(parts[3]);
				double count = 1;
				stationSumCount.put(parts[0], new Info(tmax, count));
				/*for (int i = 1; i <= 17; i++)
		           fibonacci(i);*/
			}
			else{
				Info in = stationSumCount.get(parts[0]);
				in.tmax += Double.parseDouble(parts[3]);
				in.count += 1;
				for (int i = 1; i <= 17; i++)
		           fibonacci(i);

			}
		}
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

class CoarseLockAvg extends Thread{
	int start, stop;
	public CoarseLockAvg(int start, int stop){
		this.start = start;
		this.stop = stop;
	}
	public void run(){
		for(int i = start; i < stop; i++){
			CoarseLock.computeAvgTmax(i);
		}
	}
}
