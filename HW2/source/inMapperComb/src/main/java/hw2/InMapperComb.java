package hw2;


import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InMapperComb {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "InMapCombine");

		job.setJarByClass(InMapperComb.class);
		job.setMapperClass(InMapMapper.class);
		job.setReducerClass(InMapReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

/*
 * ComputeTemp class is used to hold the aggregated values of tmax/tmin and
 * tmax/tmin 
 * */
class ComputeTemp{
	double tminSum = 0; 
	double tmaxSum = 0; 
	double tminCount, tmaxCount;

	public ComputeTemp(String type, Double temp, Double count){
		if(type.equals("TMAX")){
			tmaxSum += temp;
			tmaxCount += count;
		}
		else {
			tminSum += temp;
			tminCount += count;
		}
	}
}

class InMapMapper extends Mapper<Object, Text, Text, Text>{
	/*
	 * The tmap data structure is used to hold the stationID and ComputeTemp object
	 * as the key value pair. ComputeTemp stores the aggregated tmax/tmin values
	 * and tmax/tmin counts
	 * */
	private Map<String, ComputeTemp> tmap;


	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		tmap = new HashMap<String, ComputeTemp>();

	}

	/*
	 * For every input <key, value> record we store the stationID and ComputeTemp object
	 * in the 'tmap' HashMap as a key value pair respectively
	 * */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String input = value.toString();
		if((input.contains("TMAX")) || (input.contains("TMIN"))){
			String parts[] = input.split(",");
			double temp = Double.parseDouble(parts[3]);
			double count = 1;
			/*
			 * check if the record exists, if not then instantiate ComputeTemp
			 * and store into HashMap as <K,V>
			 * */
			if(tmap.get(parts[0]) == null){
				tmap.put(parts[0], new ComputeTemp(parts[2], temp, count));
			}
			else{
				/*
				 * If the record already exists then fetch the value for that key
				 * and add the current temp to existing tmaxSum / tminSum based on 
				 * the tempType, increment the tmaxCount/tminCount by 1 accordingly
				 * */
				ComputeTemp cs = tmap.get(parts[0]);
				if(parts[2].equals("TMAX")){
					cs.tmaxSum += temp;
					cs.tmaxCount += count;
				} else {
					cs.tminCount += count;
					cs.tminSum += temp;
				}
			}
		}
	}


	/*
	 * Cleanup iterates over the HashMap and computes the mean tmax and tmin. 
	 * It emits the stationID as the key and (meanTmin,meanTmax) as value
	 * */
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {

		@SuppressWarnings("rawtypes")
		Iterator itr = tmap.entrySet().iterator();

		while(itr.hasNext()){
			@SuppressWarnings("rawtypes")
			Map.Entry pair = (Map.Entry)itr.next();
			String id = pair.getKey().toString();
			ComputeTemp result = (ComputeTemp) pair.getValue();

			context.write(new Text(id), new Text(result.tminSum + "," + result.tminCount + "," + result.tmaxSum + "," + result.tmaxCount));

		}
	}
}

/*
 * Reducer iterates on all the values for a specific station and aggregates them to
 * compute the final meanTmax and meanTmin for the specific stationID.
 * It emits stationID as key and (meanTmin, meanTmax) as the value
 * */
class InMapReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double tminS=0; double tmaxS=0; double minC = 0; double maxC = 0;

		for(Text val : values){
			String p[] = val.toString().split(",");
			tminS += Double.parseDouble(p[0]);
			minC  += Double.parseDouble(p[1]);
			tmaxS += Double.parseDouble(p[2]);
			maxC  += Double.parseDouble(p[3]);
		}
		double avgTmin = tminS / minC;
		double avgTmax = tmaxS / maxC;
		context.write(key, new Text(String.format( "%.2f", avgTmin) + ", " + String.format( "%.2f", avgTmax)));

	}

}
