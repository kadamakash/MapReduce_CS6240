package hw3; 

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* The input that this mapper gets is from output of the PageRank 
 * computations on the 10th iteration. The Map emits Page rank as
 * the key which is a DoubleWritable and the Page name as the value
 * which is a Text. This job is basically to get the top hundread 
 * ranked pages so, I store it in a HashMap "topPages" with Key as 
 * pagename and page rank as value. 
 * */
public class OutputMapper extends Mapper<Object, Text, DoubleWritable, Text>{

	private Map<String, Double> topPages;

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {

		topPages = new HashMap<String, Double>();
	}

	/*Map method updates the HashMap with key as page and page rank as value*/
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String parts[] = value.toString().split("->");
		topPages.put(parts[0], Double.parseDouble(parts[2]));

	}

	/* The cleanup sorts the hashmap in descending order of the page rank values
	 * and it emits only the top 100 pages with the page rank value. the key 
	 * emitted is page rank and the value is the page name*/
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {

		List<Entry<String,Double>> sortedEntries = new ArrayList<Entry<String,Double>>(topPages.entrySet());

		Collections.sort(sortedEntries, 
				new Comparator<Entry<String,Double>>() {
			@Override
			public int compare(Entry<String,Double> de1, Entry<String,Double> de2) {
				return de2.getValue().compareTo(de1.getValue());
			}
		});

		Iterator<Entry<String,Double>> itr = sortedEntries.iterator();
		int i = 1;
		while(itr.hasNext() && i<101){
			i++;
			Map.Entry<String,Double> pair = (Map.Entry<String,Double>)itr.next();
			Double pageR = pair.getValue();
			String page = pair.getKey();

			context.write(new DoubleWritable(pageR), new Text(page));
		}
	}

}
