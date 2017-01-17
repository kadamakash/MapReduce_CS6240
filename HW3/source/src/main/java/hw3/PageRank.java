package hw3;


import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {

	private static long totalPages;
	private static long df = 0L;

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Path input;
		Path outputGraph;

		input = new Path(otherArgs[0]);
		outputGraph = new Path(otherArgs[1]);

		// call for first Map-Reduce Job
		readData(conf, input, outputGraph);

		// call to Second Map-Reduce Job which is run 10 times
		int i = 0;
		@SuppressWarnings("unused")
		boolean done = false;
		while (i < 10) {
			done = computePR(conf, i++, input);
		}

		// call for the Third Map-Reduce Job to find the top 100 pages by page rank
		writeOutput(conf, input);

	}

	public static void readData(Configuration conf, Path input, Path outputGraph) throws Exception{
		Job job = Job.getInstance(conf, "input reader");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(InputParseMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, outputGraph);
		job.waitForCompletion(true);

		/*boolean check = job.waitForCompletion(true);
		if (!check) {
			throw new Exception("Error");
		}*/

		// counter that keeps the count of all the pages in out dataset, this values
		// changes in the next iteration
		totalPages = job.getCounters().findCounter(CounterGroup.NumberOfPages).getValue();

	}

	public static boolean computePR(Configuration conf, int i, Path p) throws Exception{

		conf.setLong("noOfPages", totalPages);
		conf.setInt("iterator", i);
		conf.setLong("dandlingfactor", df); //dangling factor
		StringBuilder input = new StringBuilder();
		StringBuilder output = new StringBuilder();

		Job job = Job.getInstance(conf, "pageRank iterator");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(GraphData.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);


		/*String parts[] = p.toString().split("/");
		if(i==0){
			//parts[6] = "output/part-r-00000";
			parts[6] = "output";
		}
		else{
			//parts[6] = "outputJob2/PageRank" + (i-1) +"/part-r-00000";
			parts[6] = "outputJob2/PageRank" + (i-1);
		}

		for(String str : parts){
			input.append(str);
			input.append("/");
		}

		String in = input.toString().substring(0, input.length() - 1);*/
		String in;
		if(i==0){
		 in = "/home/akash/Desktop/CS6240/hw3/HW3Submission/source/output";
		}
		else 
			in = "/home/akash/Desktop/CS6240/hw3/HW3Submission/source/PageRank-" + (i-1);
		/*String p2[] = p.toString().split("/");
		p2[6] = "outputJob2/PageRank" + i;         

		for(String s: p2){
			output.append(s);
			output.append("/");
		}

		String out = output.toString().substring(0, output.length() - 1);*/
		String out = "/home/akash/Desktop/CS6240/hw3/HW3Submission/source/PageRank-" + i;
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		job.waitForCompletion(true);

		df = job.getCounters().findCounter(CounterGroup.DanglingFactor).getValue();
		totalPages = job.getCounters().findCounter(CounterGroup.NumberOfPages).getValue();

		return false;


	}

	public static void writeOutput(Configuration conf, Path input) throws Exception{
		Job job = Job.getInstance(conf, "pageRank output");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(OutputMapper.class);
		job.setReducerClass(OutputReducer.class);
		job.setSortComparatorClass(PageRankSort.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);

		StringBuilder in = new StringBuilder();
		StringBuilder out3 = new StringBuilder();

		/*String parts[] = input.toString().split("/");
		//parts[6] = "outputJob2/PageRank10/part-r-00000";
		parts[6] = "outputJob2/PageRank10";
		for(String s: parts){
			in.append(s);
			in.append("/");
		}
		String in1 = in.toString().substring(0, in.length() - 1);*/
		
		String in1 = "/home/akash/Desktop/CS6240/hw3/HW3Submission/source/PageRank-10";

		FileInputFormat.addInputPath(job, new Path(in1));

		/*String parts1[] = input.toString().split("/");
		parts1[6] = "outputJob3";
		for(String s1: parts1){
			out3.append(s1);
			out3.append("/");
		}
		String out1 = out3.toString().substring(0, out3.length() - 1);*/
		
		String out1 = "/home/akash/Desktop/CS6240/hw3/HW3Submission/source/job-3-output";
		FileOutputFormat.setOutputPath(job, new Path(out1));

		job.waitForCompletion(true);
	}

}

