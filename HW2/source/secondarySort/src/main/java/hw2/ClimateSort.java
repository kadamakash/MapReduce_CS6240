package hw2;

import java.io.DataInput;   
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ClimateSort {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ClimateSort");

		job.setJarByClass(ClimateSort.class);
		//job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(KeyGrouping.class);
		//job.setSortComparatorClass(CompositeKeyComparator.class);
		job.setMapperClass(ClimateSortMapper.class);
		job.setReducerClass(ClimateSortReducer.class);
		job.setMapOutputKeyClass(IdAndYear.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		//job.setNumReduceTasks(2);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
}

/* This class is used to create the composite key which is the combination
 * of stationID and the year from the records
 * */
class IdAndYear implements WritableComparable<IdAndYear>{
	public String stationID;
	public int year;

	public IdAndYear(){	
	}

	public IdAndYear(String stationID, int year){
		this.stationID = stationID;
		this.year = year;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(stationID);
		out.writeInt(year);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.stationID = in.readUTF();
		this.year = in.readInt();
	}

	/* this method checks whether the stationID is equal, if it
	 * is equal then it checks the equality of the year so as
	 * to get the temperature records by ascending order of the
	 * year for a particular station
	 * */
	@Override
	public int compareTo(IdAndYear other){
		int result = stationID.compareTo(other.stationID);
		if(result == 0){
			if(year > other.year){
				result = 1;
			}
			else if( year == other.year){
				result = 0;
			}
			else 
				result = -1;
		}
		return result;
	}

	public int getYear() {
		return year;
	}

	public String getStationID() {
		return stationID;
	}
}

/* This is the Composite Key comparator class which provides the secondary
 * sort functionality. It compares the stationID first if they are equal
 * then the years are compared for equality so that the temperature records 
 * for the stations are orderd in ascending order of the year
 * */
class CompositeKeyComparator extends WritableComparator{

	public CompositeKeyComparator() {
		super(IdAndYear.class, true);
	}

	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		IdAndYear k1 = (IdAndYear) wc1;
		IdAndYear k2 = (IdAndYear) wc2;

		int result = (k1.getStationID()).compareTo(k2.getStationID());
		if(result == 0) {
			if(k1.getYear() > k2.getYear()){
				result = 1;
			}
			else if( k1.getYear() == k2.getYear()){
				result = 0;
			}
			else 
				result = -1;
		}
		return result;
	}		
}


/* This is the key grouping class which is called after the map phase and it groups
 * all the record only by the stationID(natural key) so that, records belonging
 * to the same stationID go to the same reduce function
 * */
class KeyGrouping extends WritableComparator{

	public KeyGrouping() {
		super(IdAndYear.class, true);
	}

	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable wc1, WritableComparable wc2){
		IdAndYear i1 = (IdAndYear) wc1;
		IdAndYear i2 = (IdAndYear) wc2;

		return i1.getStationID().compareTo(i2.getStationID());
	}
}


/*KeyPartitoner partitions based on the key stationID*/
class KeyPartitioner extends Partitioner<IdAndYear, Text>{

	public int getPartition(IdAndYear key, Text value, int nrt) {
		int hash = key.getStationID().hashCode();
		int partition = hash % nrt;
		return partition;
	};
}


/* This class is used to compute the tmax/ tmin sum and their counts
 * respectively
 * */
class ComputeSum{
	double tminSum = 0; 
	double tmaxSum = 0; 
	double tminCount, tmaxCount;

	public ComputeSum(String type, Double temp, Double count){
		if(type.equals("TMAX")){
			tmaxSum += temp;
			tmaxCount += count;
		}
		else {
			tminSum += temp;
			tminCount += count;
		}
	}
}

/* The Mapper class performs in map combining so as to reduce
 * the workload in the reduce phase. To do this a hashmap is
 * maintained which stores the composite key as key and the ComputeSum
 * object which computes the tmin / tmax sum and counts respectively.
 * It emits the composite key (StationID, year) and the computed 
 * tmaxSum, tminSum and their counts respectively
 * */
class ClimateSortMapper extends Mapper<Object, Text, IdAndYear, Text>{

	private Map<IdAndYear, ComputeSum> map;  

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		map = new HashMap<IdAndYear, ComputeSum>();
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString(); // converting the input line to string
		if(line.contains("TMAX") || line.contains("TMIN")){ // considering only TMAX and TMIN values
			String[] parts = line.split(","); 
			String stationID = parts[0];
			int year = Integer.parseInt(parts[1].substring(0, 4));
			double temp = Double.parseDouble(parts[3]);
			double count = 1;

			IdAndYear compKey = new IdAndYear(stationID, year);

			if(map.get(compKey) == null){
				map.put(compKey, new ComputeSum(parts[2], temp, count));
			}
			else{
				/*
				 * If the record already exists then fetch the value for that key
				 * and add the current temp to existing tmaxSum / tminSum based on 
				 * the tempType, increment the tmaxCount/tminCount by 1 accordingly
				 * */
				ComputeSum cs = map.get(compKey);
				if(parts[2].equals("TMAX")){
					cs.tmaxSum += temp;
					cs.tmaxCount += count;
				} else {
					cs.tminCount += count;
					cs.tminSum += temp;
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {

		@SuppressWarnings("rawtypes")
		Iterator itr = map.entrySet().iterator();

		while(itr.hasNext()){
			@SuppressWarnings("rawtypes")
			Map.Entry pair = (Map.Entry)itr.next();
			IdAndYear id = (IdAndYear) pair.getKey();
			ComputeSum result = (ComputeSum) pair.getValue();

			context.write(id, new Text(result.tminSum + "," + result.tminCount + "," + result.tmaxSum + "," + result.tmaxCount));

		}
	}
}

/* In reducer the aggregate on all the tmin/tmax sum and count is 
 * computed and the average for the specific station and year in ascending
 * order is emitted. The order is maintained for two reasons 
 * 1) we are grouping the records based only on the stationID
 * 2) as we have overridden the compareTo() previously all the years
 * will be in ascending order for a particular station
 * So, the reducer will get the input in the sorted order
 * In order to ensure the averages are computed correctly for every 
 * year and emitted appropriately we keep a track of the current year
 * and previous year in the records for which the data is aggregated
 * As soon as we see there is a change in year we append the avg that
 * we have computed so far for the particular year. Please look for
 * comments inside the reduce method to see how the records are appended 
 * */
class ClimateSortReducer extends Reducer<IdAndYear, Text, Text, NullWritable>{

	Text result = new Text();
	Text ke = new Text();

	@Override
	protected void reduce(IdAndYear key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double tmaxS = 0; double tminS = 0; double tmaxC = 0; double tminC = 0;
		StringBuilder stb = new StringBuilder();
		int curYear = key.getYear();
		int prevYear  = curYear;

		for(Text val : values){
			prevYear = curYear;
			curYear = key.getYear();
			String p[] = val.toString().split(",");
			double tminS1 = Double.parseDouble(p[0]);
			double tminC1  = Double.parseDouble(p[1]);
			double tmaxS1 = Double.parseDouble(p[2]);
			double tmaxC1  = Double.parseDouble(p[3]);

			/* as the reducer gets records in the sorted order, this part
			 * appends the record to the string as soon as it sees a change
			 * in the year and initializes the tminC/tmaxS, tminS/tmaxC variables to 0 
			 * to again start aggregating the tmin/tmax values for the next year
			 * */
			if(curYear != prevYear){
				stb.append("(");
				stb.append(prevYear +", ");
				stb.append((tminC == 0) ? "No Record" : String.format("%.2f", tminS/tminC));
				stb.append(", ");
				stb.append((tmaxC == 0) ? "No Record" : String.format("%.2f", tmaxS/tmaxC));
				stb.append(")");
				stb.append(", ");

				tminC = 0; tminS = 0;
				tmaxC = 0; tmaxS = 0;

			}
			/* compute the sum until year of the current incoming record
			 * does not match the previous year*/
			tminC += tminC1;
			tminS += tminS1;
			tmaxC += tmaxC1;
			tmaxS += tmaxS1;

		}
		/* this part handles the case when computing the avg tmin and tmax 
		 * for the final year in the record as there is no next year in the 
		 * records
		 * */
		if(curYear == prevYear){
			stb.append("(");
			stb.append(prevYear +", ");
			stb.append((tminC == 0) ? "No Record" : String.format("%.2f",tminS/tminC));
			stb.append(",");
			stb.append((tmaxC == 0) ? "No Record" : String.format("%.2f",tmaxS/tmaxC));
			stb.append(")");
		}
		
		// finally emit the stationID with all the corresponding avg temp values
		context.write(new Text(key.stationID +" "+ "{" + stb + "}"), null);

	}
}


// purposely leaving the following commented reduce code for performance comparison
/*@Override
protected void reduce(IdAndYear key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
	double tmaxSum = 0; double tminSum = 0; double tmaxCount = 0;
	double tminCount = 0;
	StringBuilder stb = new StringBuilder();
	int curYear = key.getYear();
	int prevYear  = curYear;

	for(Text val : values){
		prevYear = curYear;
		curYear = key.getYear();
		String parts[] = val.toString().split(",");
		String type = parts[2];
		double tmp = Double.parseDouble(parts[3]);
		double count = 1;

		if(curYear != prevYear){
			stb.append("(");
			stb.append(prevYear +", ");
			stb.append((tminCount == 0) ? "No Record" : String.format("%.2f", tminSum/tminCount));
			stb.append(", ");
			stb.append((tmaxCount == 0) ? "No Record" : String.format("%.2f", tmaxSum/tmaxCount));
			stb.append(")");
			stb.append(", ");

			tminCount = 0; tminSum = 0;
			tmaxCount = 0; tmaxSum = 0;

		}

		if(type.equals("TMIN")){
			tminSum += tmp;
			tminCount += count;
		}
		else{
			tmaxSum += tmp;
			tmaxCount += count;
		}
	}

	if(curYear == prevYear){
		stb.append("(");
		stb.append(prevYear +", ");
		stb.append((tminCount == 0) ? "No Record" : String.format("%.2f",tminSum/tminCount));
		stb.append(",");
		stb.append((tmaxCount == 0) ? "No Record" : String.format("%.2f",tmaxSum/tmaxCount));
		stb.append(")");
	}
	context.write(new Text(key.stationID +" "+ "{" + stb + "}"), null);
}*/