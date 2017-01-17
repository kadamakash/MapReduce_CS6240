package hw5;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top100Reducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{

	int i, k;
	private Pattern filePattern = Pattern.compile("part-\\w-[0-9]*");
	private static HashMap<Long,String> page_link;

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		page_link = new HashMap<Long,String>();
		i = 0; 
		k = context.getConfiguration().getInt("cnt", -10);
		if(k == -10)
			throw new Error("Error in getting the count to print");

		File[] f = new File("./someName").listFiles();

		for (int i=0;i<f.length;i++){
			Matcher matcher = filePattern.matcher(f[i].getName());
			if(!f[i].getName().contains(".")&&matcher.find()){
				String line="";
				BufferedReader bufferedReader = new BufferedReader(new FileReader(f[i].getPath()));
				while((line = bufferedReader.readLine()) != null) {                        
					if(line.startsWith("Mapping")){
						page_link.put(Long.parseLong(line.split("\t")[1].split(":")[1]),line.split("\t")[1].split(":")[0]);
					}
				}   
				bufferedReader.close(); 
			}
		}   
	}

	@Override
	protected void reduce(DoubleWritable key, Iterable<Text> values,
			Context context)
					throws IOException, InterruptedException {
		for (Text val : values) {
			if(k < i)
				break;
			context.write(new Text(page_link.get(Long.parseLong(val.toString()))),new DoubleWritable(1- Double.parseDouble(key.toString())));
			i++;
		}

	}
	
}
