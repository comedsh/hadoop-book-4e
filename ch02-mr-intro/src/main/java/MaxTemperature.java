
// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {
	
	/**
	 * 运行前的注意事项，将 /Users/mac/workspace/mine/hadoop/hadoop-book/weather-data 拷贝到 hadoop fs /input/ncdc 中
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
//		if (args.length != 2) {
//			
//			System.err.println("Usage: MaxTemperature <input path> <output path>");
//			
//			System.exit(-1);
//			
//		}

		Job job = Job.getInstance();
		
		job.setJarByClass(MaxTemperature.class);
		
		job.setJobName("Max temperature");

//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileInputFormat.addInputPath(job, new Path("/input/ncdc"));
		
		FileOutputFormat.setOutputPath(job, new Path("/output"));
		
		job.setMapperClass(MaxTemperatureMapper.class);
		
		job.setReducerClass(MaxTemperatureReducer.class);

		job.setOutputKeyClass(Text.class);
		
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
// ^^ MaxTemperature
