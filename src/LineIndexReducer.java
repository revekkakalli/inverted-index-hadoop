// (c) Copyright 2009 Cloudera, Inc.
// Hadoop 0.20.1 API Updated by Marcello de Sales (marcello.desales@gmail.com)
import java.io.IOException;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * 
 * @author epl451
 *
 * LineIndexReducer Takes a list of filename@offset entries for a single word and concatenates them into a list.
 */
public class LineIndexReducer extends Reducer<Text, IndexMapRecordWritable, Text, IndexRecordWritable> {
 
    public LineIndexReducer() {
    }
 
    /**
     * @param key is the key of the mapper
     * @param values are all the values aggregated during the mapping phase
     * @param context contains the context of the job run
     *
     *      PRE-CONDITION: receive a list of <"word", "filename@offset"> pairs
     *        <"marcello", ["a.txt@3345", "b.txt@344", "c.txt@785"]>
     *
     *      POST-CONDITION: emit the output a single key-value where all the file names
     *        are separated by a comma ",".
     *        <"marcello", "a.txt@3345,b.txt@344,c.txt@785">
     */
    @Override
    protected void reduce(Text key, Iterable<IndexMapRecordWritable> values, Context context) throws IOException, InterruptedException {
 //       StringBuilder valueBuilder = new StringBuilder();
        
//        for (IndexMapRecordWritable val : values) {
//            valueBuilder.append(val);
//            valueBuilder.append(",");
//
//        }
     
        IndexRecordWritable result=new IndexRecordWritable(values);
        context.write(key, result);
        
        //write the key and the adjusted value (removing the last comma)
        //context.write(key, new IndexRecordWritable(valueBuilder.substring(0, valueBuilder.length() - 1)));
        //valueBuilder.setLength(0);
    }
}