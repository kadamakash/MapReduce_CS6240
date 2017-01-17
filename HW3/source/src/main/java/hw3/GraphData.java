package hw3;

import java.io.DataInput; 
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/* This class does the processing which is needed in the job2
 * Mapper and also the Reducer to compute the page Rank*/
public class GraphData implements Writable{

	Boolean isPRdata;
	Double pageRVal;
	Long outlinkCount;
	Text outlinkList;
	
	public GraphData() {
		this.isPRdata = false;
		this.pageRVal = 0.0;
		this.outlinkCount = 0l;
		this.outlinkList = new Text();
		
	}
	
	public GraphData(Boolean isPRdata, Double pageRVal, Long outlinkCount){
		this.isPRdata = isPRdata;
		this.pageRVal = pageRVal;
		this.outlinkCount = outlinkCount;
		outlinkList = new Text();
	}
	
	public GraphData(Boolean isPRdata, String outlinkList){
		this.isPRdata = isPRdata;
		this.outlinkList = new Text(outlinkList);
		pageRVal = 0.0;
		outlinkCount = 0L;
	}
	
	
	public Boolean getIsPRdata() {
		return isPRdata;
	}

	public void setIsPRdata(Boolean isPRdata) {
		this.isPRdata = isPRdata;
	}

	public Double getPageRVal() {
		return pageRVal;
	}

	public void setPageRVal(Double pageRVal) {
		this.pageRVal = pageRVal;
	}

	public Long getOutlinkCount() {
		return outlinkCount;
	}

	public void setOutlinkCount(Long outlinkCount) {
		this.outlinkCount = outlinkCount;
	}

	public String getOutlinkList() {
		return outlinkList.toString();
	}

	public void setOutlinkList(String outlinkList) {
		this.outlinkList = new Text(outlinkList);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((isPRdata == null) ? 0 : isPRdata.hashCode());
		result = prime * result
				+ ((outlinkCount == null) ? 0 : outlinkCount.hashCode());
		result = prime * result
				+ ((outlinkList == null) ? 0 : outlinkList.hashCode());
		result = prime * result
				+ ((pageRVal == null) ? 0 : pageRVal.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GraphData other = (GraphData) obj;
		if (isPRdata == null) {
			if (other.isPRdata != null)
				return false;
		} else if (!isPRdata.equals(other.isPRdata))
			return false;
		if (outlinkCount == null) {
			if (other.outlinkCount != null)
				return false;
		} else if (!outlinkCount.equals(other.outlinkCount))
			return false;
		if (outlinkList == null) {
			if (other.outlinkList != null)
				return false;
		} else if (!outlinkList.equals(other.outlinkList))
			return false;
		if (pageRVal == null) {
			if (other.pageRVal != null)
				return false;
		} else if (!pageRVal.equals(other.pageRVal))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.isPRdata = in.readBoolean();
		this.pageRVal = in.readDouble();
		this.outlinkCount = in.readLong();
		outlinkList.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isPRdata);
		out.writeDouble(pageRVal);
		out.writeLong(outlinkCount);
		outlinkList.write(out);
		
	}

	
}
