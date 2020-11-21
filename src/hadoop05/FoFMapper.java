package hadoop05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FoFMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	//tom hello hadoop cat
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] strs = value.toString().split(" ");    
		for (String string : strs) {
			context.write(new Text(string), new IntWritable(1));
		}
	}
}
