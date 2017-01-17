package hw2;

import java.io.IOException; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class NoCombiner {

	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "NoCombiner");

		job.setJarByClass(NoCombiner.class);
		job.setMapperClass(NoCombinerMapper.class);
		job.setReducerClass(NoCombinerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}

class NoCombinerMapper extends Mapper<Object, Text, Text, Text>{
	private Text t = new Text();
	private Text id = new Text();

	/*
	 * Map takes an input key and value, where the value is a text and is considered
	 * only if contains TMAX or TMIN values. It emits stationID along with the entire
	 * line emit(id, value)
	 * */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString(); // converting the input line to string
		if(line.contains("TMAX") || line.contains("TMIN")){ // considering only TMAX and TMIN values
			String[] parts = line.split(","); 
			String stationID = parts[0];

			t.set(line);
			id.set(stationID);
			context.write(id, t);
		}

	}
}

/*
 * The reducer input <K,V> are both of Text type. For all the values of a specific 
 * key it is processed separately based on the 'TMAX' 'TMIN'. Accordingly the averages
 * are computed for both tmax and tmin and the reduce emits(stationID, (meanTmin, meanTmax))
 * both <K,V> are of Text type.
 * */
class NoCombinerReducer extends Reducer<Text, Text, Text, Text>{

	private Text avg = new Text();
	double nan = -11111;

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double sumtmin = 0; double sumtmax = 0;
		double counttmin = 0; double counttmax = 0;
		double tmin, tmax; String result;

		for(Text val : values){
			String l = val.toString();
			String parts[] = l.split(",");

			if(parts[2].equals("TMIN")){
				sumtmin += Double.parseDouble(parts[3]);
				counttmin++;
			}
			else{
				sumtmax += Double.parseDouble(parts[3]);
				counttmax++;
			}
		}
		// to handle the NaN condition
		if(counttmin == 0 && sumtmin == 0){ 
			tmin = nan;
		}
		else {
			tmin = sumtmin/counttmin;
		}
		// to handle the NaN condition
		if(counttmax == 0 && sumtmax == 0){
			tmax = nan;
		}
		else {
			tmax = sumtmax/counttmax;
		}
		//result = String.format("%.2f", tmin) + ", " + String.format("%.2f", tmax); 
		result = resultFormatter(tmin, tmax);
		avg.set(result);
		context.write(key, avg);

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