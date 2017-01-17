package hw5;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class PageRank {
	
	public static int total_nodes = 0;
    public static int dangling_nodes = 0;
    
    public static void main(String args[]) throws Exception{
    	Configuration conf = new Configuration();
    	int i = 0; int j = 0;
    	int totalIteration = 10;
    	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    	 if (otherArgs.length < 2) {
             System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
             System.exit(2);
         }
    	 
    	 // read the wiki bz2 compressed file
    	 readData(conf,otherArgs);
    	 
    	 // first iteration of page rank 
    	 long danglingFactor = computePRitr1(conf, otherArgs[1]+"/outlinks", otherArgs[1]+"/MM", otherArgs[1], i+1);
    	 pr1(conf, otherArgs[1]+"/MM", otherArgs[1]+"/iteration"+(i+1));
    	 if(danglingFactor == -1)
    		 throw new Error("Error in iteration 1");
    	 
    	 // rest 9 iteration of page rank
    	 for(i=1; i<totalIteration; i++){
    		 danglingFactor = computePR(conf, otherArgs[1]+"/MM", otherArgs[1]+"/iteration"+(i+1), danglingFactor, otherArgs[1]+"/iteration"+i);
    		 if(danglingFactor == -1)
    			 throw new Error("Error in iteration" + i);
    	 }
    	 
	System.out.println("calling top 100");
    	 //get the top 100 page records
    	 getTop100Pages(conf, i, 100, otherArgs[1], otherArgs[1]+"/outlinks");
	System.out.println("end");
    	 
    	 
    }
    
    public static void readData(Configuration conf, String[] otherArgs) throws Exception{
    	Job job = new Job(conf, "input reader");
    	job.setJarByClass(PageRank.class);
        job.setMapperClass(InputParseMapper.class);
        job.setReducerClass(InputParseReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]+"/outlinks"));
        int exitVal = (job.waitForCompletion(true) ? 0 : 1);
        if(exitVal != 1){
        	for(Counter counter : job.getCounters().getGroup(InputParseMapper.PAGE_COUNTER_GROUP)){
        		if(counter.getDisplayName().equals("Total"))
                    total_nodes = (int)counter.getValue();
                if(counter.getDisplayName().equals("Dangling"))
                    dangling_nodes = (int)counter.getValue();
        	}
        }
    }
    
    public static long computePRitr1(Configuration conf,String inPath,String outPath,String other_args,int i) throws Exception{
    	conf.setInt("total_nodes", total_nodes);
        conf.setInt("dangling_nodes", dangling_nodes); 
        Job job = Job.getInstance(conf, "page rank itr 1");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PRI1Mapper.class);
        job.setReducerClass(PRI1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MMatrix.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        int exitVal = (job.waitForCompletion(true) ? 0 : 1);
        if (exitVal != 1) {
            for (Counter counter : job.getCounters().getGroup(PRI1Reducer.DANGLING_COUNTER_GROUP)) {
                System.out.println(counter.getDisplayName() + "\t" + (double)counter.getValue()/100000000);
                if(counter.getDisplayName().equals("dangling"))
                    return counter.getValue();
            }
            return -1;
        }
        else
            return -1;
    }
    
    // computes the page rank iteration 1
    public static long pr1(Configuration conf,String inPath,String outPath) throws Exception{
    	conf.setInt("total_nodes", total_nodes);
        conf.setInt("dangling_nodes", dangling_nodes);
        Job job = Job.getInstance(conf, "page rank i1");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PRInitMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        int exitVal = (job.waitForCompletion(true) ? 0 : 1);
        // check return code and then return the value of the counter
        if (exitVal == 0) {
            for (Counter counter : job.getCounters().getGroup(PRI1Reducer.DANGLING_COUNTER_GROUP)) {
                if(counter.getDisplayName().equals("dangling"))
                    return counter.getValue();
            }
            return -1;
        }
        else
            return -1;
    }

    // compute the page rank for the remaining 9 iterations
    @SuppressWarnings("deprecation")
	public static long computePR(Configuration conf,String inPath,String outPath,Long danglingFactor,String cache) throws Exception{
    	conf.setInt("total_nodes", total_nodes);
        conf.setInt("dangling_nodes", dangling_nodes);
        conf.setLong("df", danglingFactor);
        Job job = Job.getInstance(conf, "page rank rest iter");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(ComputePRMapper.class);
        job.setReducerClass(ComputePRReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MMatrix.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String new_cache = cache+"#someName";
        //System.out.println(new_cache);
        //DistributedCache.addCacheArchive(new URI(new_cache), conf);
        job.addCacheArchive(new URI(new_cache));
        //System.out.println(new_cache+"after caching");
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        int exitValue = (job.waitForCompletion(true) ? 0 : 1);

        // check return code and then return the value of the counter
        if (exitValue == 0) {
            for (Counter counter : job.getCounters().getGroup(PRI1Reducer.DANGLING_COUNTER_GROUP)) {
                if(counter.getDisplayName().equals("dangling"))
                    return counter.getValue();
            }
            return -1;
        }
        else
            return -1;
    }
    
    
    // get the top 100 page rank pages
    public static void getTop100Pages(Configuration conf, int i, int k, String path_given, String cache) throws Exception{
	System.out.print("Inside gettop100Pages");
    	conf.setInt("cnt", k);
    	Job job = new Job(conf, "page rank top 100");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(Top100Mapper.class);
        job.setReducerClass(Top100Reducer.class);
        job.setNumReduceTasks(1); 
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);     
        job.addCacheArchive(new URI(cache+"#someName"));
        //DistributedCache.addCacheArchive(new URI(cache+"#someName"), conf);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(path_given+"/iteration" + i));
        FileOutputFormat.setOutputPath(job, new Path(path_given+"/topPagesOutput"));
	job.waitForCompletion(true);
        
    }
}
