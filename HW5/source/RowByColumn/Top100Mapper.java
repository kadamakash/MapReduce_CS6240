package hw5;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top100Mapper extends Mapper<Text, Text, DoubleWritable, Text> {
	
	private TreeMap<Double, Long> sortedPages;
	int count = 0;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		sortedPages = new TreeMap<Double, Long>();
		count = context.getConfiguration().getInt("cnt", -10);
		if(count == -10)
            throw new Error("Error in getting number to print top records");
	}
	
	@Override
	protected void map(Text key, Text value,Context context)
			throws IOException, InterruptedException {
		sortedPages.put(((double) 1- Double.parseDouble(value.toString())),Long.parseLong(key.toString()));
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		int i = 0;
		for (Double pr : sortedPages.keySet()) {
            if(i > count)
                break;
            context.write(new DoubleWritable(pr),new Text (sortedPages.get(pr)+""));    
            i++;
        }
		
	}	

}
