package hadoop05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FoFReducer extends Reducer<Text, IntWritable, Text, Text> {
	Text rval=new Text();
	
	//tom 1
	//tom 1
	//hell 1
	@Override
	protected void reduce(Text key, Iterable<IntWritable> iterable, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable it : iterable) {
			sum += it.get();
		}
		rval.set(""+sum);
		context.write(key, rval);

	}
}
