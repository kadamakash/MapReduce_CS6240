package hw5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MMatrix implements Writable {
	
	private Character type; // to determine type of Matrix M / R
	private Boolean isDangling;
	private Long page;
	private String inLinks;
	private double pr;
	
	
	
	public MMatrix() {
		this.type = 'M';
		this.isDangling = false;
		this.page = (long)0;
		this.inLinks = "";
		this.pr = -1;
	}
	
	public MMatrix(Long page, Boolean isDangling, String inLinks, char type){
		this.type = type;
		this.isDangling = isDangling;
		this.page = page;
		this.inLinks = inLinks;
		this.pr = -1;
	}
	
	public MMatrix(Long page, double pr, char type){
		this.type = type;
		this.isDangling = false;
		this.page = page;
		this.inLinks = "";
		this.pr = pr;
	}
	
	public MMatrix(MMatrix o){
		this.type = o.getType();
		this.isDangling = o.getIsDangling();
		this.page = o.getPage();
		this.inLinks = o.getInLinks();
		this.pr = o.getPr();
	}
	
	public Character getType() {
		return type;
	}
	
	public Boolean getIsDangling() {
		return isDangling;
	}
	
	public Long getPage() {
		return page;
	}
	
	public String getInLinks() {
		return inLinks;
	}
	
	public double getPr() {
		return pr;
	}
	
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeChar(this.type);
		out.writeBoolean(this.isDangling);
		out.writeLong(this.page);
		Text inLink_txt = new Text(this.inLinks);
		inLink_txt.write(out);
		out.writeDouble(this.pr);
		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.type = in.readChar();
		this.isDangling = in.readBoolean();
		this.page = in.readLong();
		Text inLinks_text = new Text();
		inLinks_text.readFields(in);
		this.inLinks = inLinks_text.toString();
		this.pr = in.readDouble();
	}
	
	

}
