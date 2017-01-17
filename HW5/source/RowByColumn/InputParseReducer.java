package hw5;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InputParseReducer extends Reducer<Text,Text,Text,Text> {
	
	private static long count;
    private static HashMap<String,Long> pageMapping; 
    
    // setting up the k for top k documents and i for iteration
    public void setup(Context context){
        count=0;
        pageMapping = new HashMap<String,Long>();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values,
    		Context context)
    		throws IOException, InterruptedException {
    	
    	String list = values.iterator().next().toString();
        String outlinks[] = list.split(",");
        // check if mapping is present in the map or add accordingly
        if(pageMapping.containsKey(key.toString())){
            key = new Text(pageMapping.get(key.toString())+"");
        }
        else{
          
            pageMapping.put(key.toString(), count);
            key = new Text(count+"");
            count++;
        }
        // create a oulink list seperated by a delimiter
        String formattedOutlinks="";
        for(String link : outlinks){
        	if(!link.equals("")){
        		Text l = new Text(link);
        		if(pageMapping.containsKey(l.toString())){
        			formattedOutlinks = formattedOutlinks+pageMapping.get(l.toString())+",";
        		}
        		else {
        			pageMapping.put(l.toString(), count);
        			formattedOutlinks = formattedOutlinks+count+",";
        			count++;
        		}
        	}
        }
        if(formattedOutlinks.contains(",")){
        	formattedOutlinks = formattedOutlinks.substring(0, formattedOutlinks.lastIndexOf(","));
        }
        context.write(key, new Text(formattedOutlinks));
    }
    
    @Override
    protected void cleanup(Context context)
    		throws IOException, InterruptedException {
    	for(String p : pageMapping.keySet()){
    		context.write(new Text("Mapping"), new Text(p+":"+pageMapping.get(p)));
    	}
    }
}
