package hw3;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Input to the Reducer is Text as key and GraphData as Value and it emits Text as
 * the key and NullWritable as value. The reduce function computes the page rank and
 * emits the text same as the input text with the updated page rank value*/
public class PageRankReducer extends Reducer<Text, GraphData, Text, NullWritable>{

	private static long pageCount;
	private static long danglingFactor;
	final long DANGLING_FACTOR_MULTIPLIER = 100000000000L;
	HashSet<String> pageList;

	/* setting up the danglingFactor from configuration, pageCount from configuration
	 * and using a HashSet to store all the pages to get the pageCount*/
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		danglingFactor = Long.parseLong(context.getConfiguration().get("dandlingfactor")) / DANGLING_FACTOR_MULTIPLIER;
		pageCount = Long.parseLong(context.getConfiguration().get("noOfPages"));
		pageList = new HashSet<String>();

	}

	@Override
	protected void reduce(Text key, Iterable<GraphData> values, Context context)
			throws IOException, InterruptedException {

		String outlinkList = "";

		double prByCount = 0.0;
		double pageRank = 0.0;
		double alpha = 0.15;
		double randomJump = 0.0;
		double followLink = 0.0;
		StringBuilder output = new StringBuilder();

		for(GraphData d : values){
			if(!d.getIsPRdata()){
				outlinkList = d.getOutlinkList();
			}
			else{
				prByCount += d.getPageRVal()/ d.getOutlinkCount();
			}
		}

		randomJump = alpha / pageCount;

		followLink = (1 - alpha) * ((danglingFactor/pageCount) + prByCount);

		pageRank = randomJump + followLink;

		output.append(key.toString());
		output.append("->");
		output.append(outlinkList);
		output.append("->");
		output.append(pageRank);
		pageList.add(key.toString());

		context.write(new Text(output.toString()), NullWritable.get());
	}

	/* setting up the NumberOfPages counter based on the size of the HashSet which
	 * is used to store all the pages */
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		long count = pageList.size();
		context.getCounter(CounterGroup.NumberOfPages).setValue(count);
	}

}
