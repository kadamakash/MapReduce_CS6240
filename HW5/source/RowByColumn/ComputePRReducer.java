package hw5;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.FileReader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ComputePRReducer extends Reducer<Text, MMatrix, Text, Text>{
	
	public double randomHopFactor = 0.0; //initial value
	public double alpha = 0.15;
	public double currentDF;
	public double computedDF; // (alpha * df)/totalPages
	public static final String DANGLING_COUNTER_GROUP = "dangling";
	int total_nodes = 0;
	private Pattern filePattern = Pattern.compile("part-\\w-[0-9]*");
	HashMap<Long,Double> mapping = new HashMap<Long,Double>();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		currentDF = 0.0;
		total_nodes =  context.getConfiguration().getInt("total_nodes", -10);
		if (total_nodes == -10) {
            throw new Error("Error in getting total docs");
        }
		
		Long prev_df = context.getConfiguration().getLong("df", -10);
		double normPrevDF = ((double) prev_df / 100000000);
		if (prev_df == -10) {
            throw new Error("Error in getting dangling factor");
        }
		
		randomHopFactor = ((double)(1-alpha)/total_nodes);
		computedDF = ((double) ((double) alpha * normPrevDF) / total_nodes);
		
		File[] f = new File("./someName").listFiles();

		for (int i=0;i<f.length;i++){
			Matcher matcher = filePattern.matcher(f[i].getName());
			if(!f[i].getName().contains(".")&&matcher.find()){
				String line="";
				SequenceFile.Reader sr = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(new Path(f[i].getPath())));        
				Text page = new Text();
				Text page_rank = new Text();            
				while(sr.next(page, page_rank)) {
					mapping.put(Long.parseLong(page.toString()),Double.parseDouble(page_rank.toString()));
				}
				sr.close();
			}
		}  
	}
	
	
	@Override
	protected void reduce(Text key, Iterable<MMatrix> values,
			Context context)
			throws IOException, InterruptedException {
		
		
		Long page;
		double pr = 0.0;
		double prSum = 0.0;
		
		HashMap<Long, Double> inLinks = new HashMap<Long, Double>();
		boolean isM = false;
		MMatrix mm = new MMatrix();
		double finalPR;
		double randomSum = 0.0;
		
		for (MMatrix val : values){
			if(val.getType().equals('M')){
				String[] inLinksContri = val.getInLinks().split(",");
				for(String contri : inLinksContri){
					if(!contri.equals("")){
						inLinks.put(Long.parseLong(contri.split(":")[0]), Double.parseDouble(contri.split(":")[1]));
					}
					
				}
				isM = true;
				mm = new MMatrix(val);
			}
		}
		
		for(String pageContri : mm.getInLinks().split(",")){
			if(!pageContri.equals("")){
				page = Long.parseLong(pageContri.split(":")[0]);
				if(mapping.containsKey(page)){
					prSum += ((double)mapping.get(page)*inLinks.get(page));
				} 
			}
		}
		
		
		randomSum = ((double) prSum * alpha);
		
		finalPR = ((double) computedDF + randomHopFactor + randomSum);
		
		if(mm.getIsDangling()){
			currentDF += finalPR;
		}
		context.write(key, new Text(finalPR+""));
		
		
	}
	
	
	@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			long curDF = (long) (currentDF * 100000000);
			context.getCounter(DANGLING_COUNTER_GROUP, "dangling").increment(curDF);
		}	
		 
	
}
