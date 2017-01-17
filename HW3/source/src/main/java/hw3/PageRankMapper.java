package hw3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* Mapper reads the output of the first job which is a list of lines
 * comprising the "Page->(list of outlinkPages)->PageRank". Map emits 2
 * things 
 * 
 * 1) Page as the key and the list of outlinks as value(which is basically
 * a GraphData object, with Boolean value as false and the list as String)
 * 
 * 2) Emits each page from the input outlink list and a GraphData object as a value
 * comprising of Boolean as true, the Page rank for that page and the number of
 * outlink count from that page 
 * */
public class PageRankMapper extends Mapper<Object, Text, Text, GraphData>{

	Integer itr;
	long totalPages;
	final long DANGLING_FACTOR_MULTIPLIER = 100000000000L;

	/* setting up the itr from configuration and totalPages from the
	 * configuration */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		itr = context.getConfiguration().getInt("iterator", -1);
		totalPages = Long.parseLong(context.getConfiguration().get("noOfPages"));
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		double pageRVal = 0.0;

		String line = value.toString();
		String lineSplit[] = line.trim().split("->");		

		@SuppressWarnings("unused")
		String pages = lineSplit[1];
		String pageSplit = lineSplit[1].trim();

		String outlinks[] = null;
		if(!(pageSplit.length() == 0)){
			outlinks = pageSplit.split(",");
		}

		// for the first iteration set page Rank as 1/totalNoOfPages
		if( itr == 0){
			pageRVal = 1.0 / totalPages;
		}
		else{
			pageRVal = Double.parseDouble(lineSplit[2]);
		}

		// checking if outlink list is empty to set the Dangling Factor counter
		if(outlinks == null){
			context.getCounter(CounterGroup.DanglingFactor).increment((long)((Double.parseDouble(lineSplit[2])) * DANGLING_FACTOR_MULTIPLIER));
		}
		else {
			for(String page: outlinks){
				context.write(new Text(page), new GraphData(true, new Double(pageRVal), new Long(outlinks.length)));
			}
		}
		context.write(new Text(lineSplit[0]), new GraphData(false, lineSplit[1]));


	}

}
