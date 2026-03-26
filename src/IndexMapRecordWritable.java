/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 
 * @author epl451
 * 
 * 
 * @field offset
 * @field docid
 */
public class IndexMapRecordWritable implements Writable {

	// Private fields need to be Writable
	private LongWritable offset;
	private Text docid;

	/**
	 * 
	 * @return Writable long
	 */
	public LongWritable getOffsetWritable() {
		return offset;
	}

	/**
	 * 
	 * @return Writable string
	 */
	public Text getDocidWritable() {
		return docid;
	}

	/**
	 * Value from writable
	 * @return long
	 */
	public long getOffset() {
		return offset.get();
	}

	/**
	 * Value from writable
	 * @return long
	 */
	public String getDocid() {
		return docid.toString();
	}

	/**
	 * Default constructor
	 */
	public IndexMapRecordWritable() {
		this.offset = new LongWritable();
		this.docid = new Text();
	}

	/**
	 * Constructor with parameters
	 * 
	 * @param offset
	 * @param docid
	 */
	public IndexMapRecordWritable(long offset, String docid) {
		this.offset = new LongWritable(offset);
		this.docid = new Text(docid);
	}

	/**
	 *  Constructor with parameters
	 * 
	 * @param indexMapRecordWritable
	 */
	public IndexMapRecordWritable(IndexMapRecordWritable indexMapRecordWritable) {
		this.offset = indexMapRecordWritable.getOffsetWritable();
		this.docid = indexMapRecordWritable.getDocidWritable();
	}

	/**
	 * Concat the fields
	 */
	@Override
	public String toString() {

		StringBuilder output = new StringBuilder();
		/**
		 * PUT YOUR CODE HERE
		 */
		output.append(docid);
		output.append("@");
		output.append(offset);
		return output.toString();

	}

	/**
	 * Serialize the fields
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		//WRITABLE.write(out);
		/**
		 * PUT YOUR CODE HERE
		 */
		 docid.write(out);
	     offset.write(out);
	}
	/**
	 * Deserialize the fields
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		//WRITABLE.readFields(in);
		/**
		 * PUT YOUR CODE HERE
		 */
		docid.readFields(in);
        offset.readFields(in);
	}

}
