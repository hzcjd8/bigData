package hadoop05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyFoF {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);
		conf.set("mapreduce.app-submission.corss-paltform", "true");
		conf.set("mapreduce.framework.name", "local");
		Job job=Job.getInstance(conf);
		job.setJarByClass(MyFoF.class);
		//---- conf
		//--map
		//job.setInputFormatClass(xxoo.class);
		job.setMapperClass(FoFMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//job.setPartitionerClass(TqPartitioner.class);
		//job.setSortComparatorClass(TqSortComparator.class);
		//job.setCombinerClass(cls);
		//--map end if;
		
		//--reduce
		//job.setGroupingComparatorClass(TqGroupingComparator.class);
		job.setReducerClass(FoFReducer.class);
		//--reduce end if;
		
		//----conf end if;
		
		Path input=new Path("/data/fofwc/input");
		FileInputFormat.addInputPath(job, input);
		
		Path output=new  Path("/data/fofwc/output");
		if(output.getFileSystem(conf).exists(output)){
			output.getFileSystem(conf).delete(output,true);
		}
		FileOutputFormat.setOutputPath(job, output );
		
		job.waitForCompletion(true)	;
	}

}
