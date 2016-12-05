
// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	static Logger logger = org.apache.log4j.Logger.getLogger( MaxTemperatureReducer.class );

	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("===============> key:").append( key.toString() ).append("; values: ");
		
		int maxValue = Integer.MIN_VALUE;
		
		for (IntWritable value : values) {
				
			maxValue = Math.max(maxValue, value.get());
			
			sb.append( value.get() ).append(",");
			
		}
		
		logger.info(sb.toString());
		
		context.write(key, new IntWritable(maxValue));
		
	}
}
// ^^ MaxTemperatureReducer
