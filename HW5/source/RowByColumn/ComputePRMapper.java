package hw5;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ComputePRMapper extends Mapper<Text, MMatrix, Text, MMatrix>{
	private int total_nodes = 0;	
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
		total_nodes = context.getConfiguration().getInt("total_nodes", -10);
        if (total_nodes == -10) {
            throw new Error("Error in getting total nodes");
        }
	}
	
	@Override
	protected void map(Text key, MMatrix value,
			Context context)
			throws IOException, InterruptedException {
		if(value.getType().equals('M'))
            context.write(key, value);
	}
	

}
