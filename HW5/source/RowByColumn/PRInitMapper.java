package hw5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// emits the page along with the inital page rank value
public class PRInitMapper extends Mapper<Text, MMatrix, Text, Text>{

	int total_nodes;
	double iniPR;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		total_nodes = context.getConfiguration().getInt("total_nodes", -10);
		if (total_nodes == -10) {
            throw new Error("Error In total");
        }
        iniPR = 1/total_nodes;
	}
	
	@Override
	protected void map(Text key, MMatrix value, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(key),new Text(iniPR+""));
	}
}
