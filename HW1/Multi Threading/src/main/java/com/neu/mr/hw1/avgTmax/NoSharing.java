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

public class NoSharing {
	static ArrayList<Long> executionTime = new ArrayList<Long>();

	// this is the main data structure which will hold all the records from
	// the thread's own data structure
	static Map<String, Info> mergedMap = new HashMap<String, Info>();
	long sum;

	public NoSharing() throws InterruptedException{
		for(int a = 0; a < 10; a++){
			long startTime = System.currentTimeMillis();

			NoSharingAvg t1 = new NoSharingAvg(0, result.size()/2);
			NoSharingAvg t2 = new NoSharingAvg(result.size()/2, result.size());
			t1.start(); 
			t1.join(); 
			t2.start();
			t2.join();
			
			// In this program each thread has its own data structure, which are
			// combined in a hashmap named mergedMap
			// mapping the records from t1 thread data structure into the
			// main merged hash map 'mergedMap'
			Map<String, Info> data = t1.getMap();
			for(String stationId : data.keySet()){
				Info in = data.get(stationId);
				if(mergedMap.get(stationId) == null){
					double tmax = in.tmax;
					double count = in.count;
					mergedMap.put(stationId, new Info(tmax, count));

				}
				else{
					Info in1 = mergedMap.get(stationId);
					in1.tmax += in.tmax;
					in1.count += in.count;

				}

			}

			// mapping the records into main mergedMap from the t2 thread's
			// data structure
			Map<String, Info> data2 = t2.getMap();
			for(String stationId : data2.keySet()){
				Info in = data2.get(stationId);
				if(mergedMap.get(stationId) == null){
					double tmax = in.tmax;
					double count = in.count;
					mergedMap.put(stationId, new Info(tmax, count));

				}
				else{
					Info in1 = mergedMap.get(stationId);
					in1.tmax += in.tmax;
					in1.count += in.count;

				}

			}

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


}
// here each thread has its own data structure 
class NoSharingAvg extends Thread{
	int start, stop;
	HashMap<String, Info> stationSumCount = new HashMap<String, Info>();
	public NoSharingAvg(int start, int stop){
		this.start = start;
		this.stop = stop;
	}
	public void run(){
		for(int i = start; i < stop; i++){

			String line = result.get(i);
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
					/*for (int j = 1; j <= 17; i++)
						fibonacci(j);*/

				}
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

	// assigning a getter to get the hashmap
	public Map<String, Info> getMap(){
		return stationSumCount;
	}


}
