package hw3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PageRankSort extends WritableComparator {

	protected PageRankSort() {
		super(DoubleWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		DoubleWritable k1 = (DoubleWritable) a;
		DoubleWritable k2 = (DoubleWritable) b;
		
		return k2.compareTo(k1);
	}
}
