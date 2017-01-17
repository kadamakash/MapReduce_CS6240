package hw5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PRI1Reducer extends Reducer<Text, MMatrix, Text, MMatrix> {
	
	
	public static final String DANGLING_COUNTER_GROUP = "dangling";
	public double danglingFactor = 0.0;
	int total_nodes = 0; int dangling_nodes = 0;
	String itrPath = "";
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		total_nodes = context.getConfiguration().getInt("total_nodes", -10);
		dangling_nodes = context.getConfiguration().getInt("dangling_nodes", -10);
		if (total_nodes == -10 || dangling_nodes == -10) {
            throw new Error("Error is nodes value");
        }
        danglingFactor = ((double)dangling_nodes * ((double)1/total_nodes));
        
	}
	
	@Override
	protected void reduce(Text key, Iterable<MMatrix> values,Context context)
			throws IOException, InterruptedException {

		boolean isDangling = false;

		String inLinks = "";
		String dl = "";
		
		// each node will either have page rank or the outlinks of the current page
		for (MMatrix val : values) {
            isDangling = isDangling || val.getIsDangling();
            if(!val.getInLinks().equals("")){
                inLinks = inLinks + dl + val.getInLinks();
                dl = ",";
            }
        }
		// create a matrix
		MMatrix mm = new MMatrix(Long.parseLong(key.toString()) , isDangling , inLinks  ,'M');
		// emiting key and a new matrix
		context.write(key, mm);
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// to maintain precision multiply by 10^8
		long DF = (long) (danglingFactor * 100000000);
		context.getCounter(DANGLING_COUNTER_GROUP,"dangling").increment(DF);
        
	}
	

}
