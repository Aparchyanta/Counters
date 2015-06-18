package hadoop.CounterExample;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CounterMapper extends Mapper<LongWritable, Text, Text, Text>{
	enum MYCOUNTER {
		INFO, WARN, DEBUG, FATAL
		}
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(value.toString().contains("INFO"))
			context.getCounter(MYCOUNTER.INFO).increment(1);
		else if(value.toString().contains("WARN"))
			context.getCounter(MYCOUNTER.WARN).increment(1);
		else if(value.toString().contains("DEBUG"))
			context.getCounter(MYCOUNTER.DEBUG).increment(1);
		else
			context.getCounter(MYCOUNTER.FATAL).increment(1);
		
		context.write(new Text("SUCCESS"), new Text("SUCCESS"));
	}
}
