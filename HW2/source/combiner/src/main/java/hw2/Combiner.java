package hw2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

public class Combiner {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Combiner");

		job.setJarByClass(Combiner.class);
		job.setMapperClass(CombMapper.class);
		job.setCombinerClass(CustomCombiner.class);
		job.setReducerClass(CombReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

class CombMapper extends Mapper<Object, Text, Text, Text>{

	/*
	 * In map I am emitting stationID as key and the value is a Text object
	 * comprising of Temperature Value + Temperature type(i.e "TMAX"/"TMIN") +
	 * count which is 1
	 * */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// converting the input line to string
		String line = value.toString(); 
		// considering only TMAX and TMIN records, ignores rest
		if(line.contains("TMAX") || line.contains("TMIN")){ 
			String[] parts = line.split(","); 
			String stationID = parts[0];
			String TType = parts[2];
			double TTval = Double.parseDouble(parts[3]);
			double count = 1;
			context.write(new Text(stationID), new Text(TTval+ "," + TType + "," + count));
		}

	}
}


class CustomCombiner extends Reducer<Text, Text, Text, Text>{

	/*
	 * In the CustomCombiner class the method iterates on all the values for a 
	 * specific station. The method computes the sumtmax, tmaxcount, sumtmin, tmincount
	 * based on the type i.e TMAX / TMIN
	 * It emits the key and aggregated value of (tmax/tmin + type + tmaxCount/tminCount)
	 * */

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double tmaxSum = 0;
		double tminSum = 0; double tmaxCount = 0; double tminCount = 0;
		for(Text v : values){
			String parts[] = v.toString().split(",");
			double temp = Double.parseDouble(parts[0]);
			String type = parts[1];
			double count = Double.parseDouble(parts[2]);
			if(type.equals("TMAX")){
				tmaxSum += temp;
				tmaxCount += count;
			}
			else {
				tminSum += temp;
				tminCount += count;
			}

		}
		context.write(key, new Text(tmaxSum + "," + "TMAX" + "," + tmaxCount));
		context.write(key, new Text(tminSum + "," + "TMIN" + "," +tminCount));

	}
}

class CombReducer extends Reducer<Text, Text, Text, Text>{

	double nan = -11111;
	/*
	 * Similar to combiner the reducer also does the same job as the CustomCombiners
	 * the only difference being after aggregating the corresponding tmax and tmin 
	 * values it computes the avgTmax and avgTmin by dividing the tmaxSum with tmaxCount
	 * and tminSum with tminCount.
	 * It emits stationID as the key and (meanTmin, meanTmax) as the value
	 * */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double tmaxSum = 0; double tminSum = 0; double avgTmax = 0; double avgTmin = 0;
		double tminCount = 0; double tmaxCount = 0;
		for(Text v : values){
			String parts[] = v.toString().split(",");
			double temp = Double.parseDouble(parts[0]);
			String type = parts[1];
			double count = Double.parseDouble(parts[2]);

			if(type.equals("TMAX")){
				tmaxSum += temp;
				tmaxCount += count;
			}
			else {
				tminSum += temp;
				tminCount += count;
			}
		}
		// check for divide by zero
		if (tmaxCount == 0 && tmaxSum == 0) {
			avgTmax = nan;
		}else{
			avgTmax = tmaxSum / tmaxCount;
		}
		// check for divide by zero
		if(tminCount == 0 && tminSum == 0){ 
			avgTmin = nan;
		}
		else {
			avgTmin = tminSum / tminCount;
		}
		String result = resultFormatter(avgTmin, avgTmax);
		context.write(key, new Text(result)); 
	}

	protected String resultFormatter(double tmin1, double tmax1){
		String res = "";
		if(tmin1 == nan && tmax1 == nan){
			res = "NULL" + ", " + "NULL";
		}
		else if(tmin1 == nan){
			res = "NULL" + ", " + String.format("%.2f", tmax1);
		}
		else if(tmax1 == nan){
			res = String.format("%.2f", tmin1) + ", " + "NULL";
		}
		else{
			res = String.format("%.2f", tmin1) + ", " + String.format("%.2f", tmax1);
		}
		return res;		
	}
}
