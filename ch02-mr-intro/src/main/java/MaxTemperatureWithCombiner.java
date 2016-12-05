
// cc MaxTemperatureWithCombiner Application to find the maximum temperature, using a combiner function for efficiency
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// vv MaxTemperatureWithCombiner
public class MaxTemperatureWithCombiner {
	
	
	public static void main(String[] args) throws Exception {
//		if (args.length != 2) {
//			System.err.println("Usage: MaxTemperatureWithCombiner <input path> " + "<output path>");
//			System.exit(-1);
//		}

		Job job = Job.getInstance();
		
		job.setJarByClass(MaxTemperatureWithCombiner.class);
		
		job.setJobName("Max temperature");

		FileInputFormat.addInputPath(job, new Path("/input/ncdc"));
		
		FileOutputFormat.setOutputPath(job, new Path("/output"));
		
		// 很有意思，这里的 Combiner 就是一次 Reducer.
		job.setMapperClass(MaxTemperatureMapper.class);
		
		job.setCombinerClass(MaxTemperatureReducer.class);
		
		job.setReducerClass(MaxTemperatureReducer.class);

		job.setOutputKeyClass(Text.class);
		
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
// ^^ MaxTemperatureWithCombiner
