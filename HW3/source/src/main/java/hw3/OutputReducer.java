package hw3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* OutputReducer i.e reducer for job3 gets input key as page rank 
 * and value as page and emits the top 100 page rank pages. The 
 * key emitted is the page rank and the value is page*/
public class OutputReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text>{

	private int count = 0; 
	
	@Override
	protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for(Text page : values){
			count++;
			context.write(key, page);
			if(count > 100){
				break;
			}
		}
	}
}
