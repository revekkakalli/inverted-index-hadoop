/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Writable;

/**
 * 
 * @author epl451
 * 
 */
public class IndexRecordWritable implements Writable {

	// Save each index record from maps
	private HashSet<IndexMapRecordWritable> tokens = new HashSet<IndexMapRecordWritable>();

	/**
	 * Default constructor
	 */
	public IndexRecordWritable() {
	}

	/**
	 * The parameter is a HashSet constructed from index records from maps
	 * 
	 * @param indexMapRecordWritables
	 */
	public IndexRecordWritable(
			Iterable<IndexMapRecordWritable> indexMapRecordWritables) {
		/**
		 * PUT YOUR CODE HERE
		 */
		for (IndexMapRecordWritable k : indexMapRecordWritables){
			tokens.add(new IndexMapRecordWritable(k.getOffset(),k.getDocid()));
		}
	}

	/**
	 * Concat all the index map records
	 */
	@Override
	public String toString() {

		StringBuilder output = new StringBuilder();

		/**
		 * PUT YOUR CODE HERE
		 */
		for(IndexMapRecordWritable t : tokens) {
	           output.append(t.toString());     
	           output.append(",");
		}
		return output.toString().substring(0,output.length()-1);

	}

	/**
	 * Serialize the fields
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		/**
		 * PUT YOUR CODE HERE
		 */
		out.writeInt(tokens.size());
		for(IndexMapRecordWritable t : tokens) {
            t.write(out);

		}	
	}

	/**
	 * Deserialize the fields
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		/**
		 * PUT YOUR CODE HERE
		 */
		int size = in.readInt();
		tokens = new HashSet<IndexMapRecordWritable>(size);
		for (int i = 0; i < size; ++i) {
             IndexMapRecordWritable value = new IndexMapRecordWritable();
             value.readFields(in);
             tokens.add(value);
         }

	}

}
