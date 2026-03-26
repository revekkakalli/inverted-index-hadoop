import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author epl451
 *
 */
public class Indexer {


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "index");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IndexRecordWritable.class);

		job.setMapperClass(LineIndexMapper.class);
		job.setReducerClass(LineIndexReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IndexMapRecordWritable.class);

		FileInputFormat.addInputPath(job, new Path(
				"hdfs://localhost:9000/user/input"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://localhost:9000/user/output4"));

		job.waitForCompletion(true);
	}
}