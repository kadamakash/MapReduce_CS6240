package hw5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PRI1Mapper extends Mapper<Object, Text, Text, MMatrix>{

	double pr = 0.0;
	double iniPR;
	int totalNodes;

	MMatrix mm;

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		int totalNodes = context.getConfiguration().getInt("total_nodes", -10);
		System.out.println("Total = " + totalNodes);
		if (totalNodes == -10) {
			throw new Error("Error in iteration");
		}
		iniPR = ((double)1/totalNodes);
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		if(!value.toString().startsWith("Mapping")){
			String[] olinks  = value.toString().split("\\t");
			Long page  = Long.parseLong(olinks[0]);

			if(olinks.length>1){ // check if there are no outlinks

				String[] out_links  = olinks[1].toString().split(",");
			
				double olCount = out_links.length; // outlink count

				// assigning the contribution og pr to outlinks
				for(String l : out_links){
					mm = new MMatrix(Long.parseLong(l) , false , page +":"+1/olCount , 'M');
					context.write(new Text(l), mm);
				}

				// emitting the current page with all it's outlinks and with page rank set to -1 , to distinguish this record
				mm = new MMatrix(page , false , "" , 'M');
				context.write(new Text(olinks[0]), mm);
			}
			else{
				// for no outlinks , just emit empty as outlinks and no page rank because of outlinks
				mm = new MMatrix(page , true , "" , 'M');
				context.write(new Text(olinks[0]), mm);
			} 
		}           

	}


}
