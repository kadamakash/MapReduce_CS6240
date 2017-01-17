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

public class FineLock {
	static ArrayList<Long> executionTime = new ArrayList<Long>();
	static Map<String, Info> stationSumCount;
	long sum;

	public FineLock() throws InterruptedException{
		for(int a = 0; a < 10; a++){
			stationSumCount = new HashMap<String, Info>();
			long startTime = System.currentTimeMillis();

			FineLockAvg t1 = new FineLockAvg(0, result.size()/2);
			FineLockAvg t2 = new FineLockAvg(result.size()/2, result.size());
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
	
	public static void computeAvgTmax(int index){
		String line = result.get(index);
		if(line.contains("TMAX")){
			String[] parts = line.split(",");
			if(stationSumCount.get(parts[0]) == null){
				double tmax = Integer.parseInt(parts[3]);
				double count = 1;
				Info newI = new Info(tmax, count);
				// getting a lock on the Info object as to avoid 2 threads to simultaneously 
				// add duplicate values for same stationID when the stationID is already 
				// not present in the hashmap before
				synchronized(newI){
					if(stationSumCount.get(parts[0]) == null){
						stationSumCount.put(parts[0], newI);
					}
					else{
						setValues(newI, parts);
					}
				}
				stationSumCount.put(parts[0], new Info(tmax, count));
			}
			else{
				Info in = stationSumCount.get(parts[0]);
				setValues(in, parts);
			}
		}

	}

	// this method is synchronized so that we get the lock on the accumulation value object
	// and not the entire data structure
	public static synchronized void setValues(Info in, String[] parts){
		in.tmax += Double.parseDouble(parts[3]);
		in.count += 1;
		for (int i = 1; i <= 17; i++)
			fibonacci(i);
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

class FineLockAvg extends Thread{
	int start, stop;
	public FineLockAvg(int start, int stop){
		this.start = start;
		this.stop = stop;
	}
	public void run(){
		for(int i = start; i < stop; i++){
			FineLock.computeAvgTmax(i);
		}
	}
}
