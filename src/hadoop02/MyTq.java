package hadoop02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyTq {
	/**需求：找出每个月气温最高的2天
       数据：
        1949-10-01 14:21:02	34c
		1949-10-01 19:21:02	38c
		1949-10-02 14:01:02	36c
		1950-01-01 11:21:02	32c
		1950-10-01 12:21:02	37c
		1951-12-01 12:21:02	23c
		1950-10-02 12:21:02	41c
		1950-10-03 12:21:02	27c
		1951-07-01 12:21:02	45c
		1951-07-02 12:21:02	46c
		1951-07-03 12:21:03	47c

	 */

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);
		Job job=Job.getInstance(conf);
		job.setJarByClass(MyTq.class);
		//---- conf
		//--map
		//job.setInputFormatClass(xxoo.class); 输入格式化
		job.setMapperClass(TqMapper.class);
		job.setMapOutputKeyClass(Tq.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setPartitionerClass(TqPartitioner.class);
		job.setSortComparatorClass(TqSortComparator.class);
		//job.setCombinerClass(cls);
		//--map end if;
		
		//--reduce
		job.setGroupingComparatorClass(TqGroupingComparator.class);
		job.setReducerClass(TqReducer.class);
		//--reduce end if;
		
		//----conf end if;
		
		Path input=new Path("/data/tq/input");
		FileInputFormat.addInputPath(job, input);
		
		Path output=new  Path("/data/tq/output");
		if(output.getFileSystem(conf).exists(output)){
			output.getFileSystem(conf).delete(output,true);
		}
		FileOutputFormat.setOutputPath(job, output );
		
		job.waitForCompletion(true)	;
	}

}
