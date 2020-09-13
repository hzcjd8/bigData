package hadoop01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class YARNtest {

	public static void main(String[] args) throws Exception {
		Configuration conf =new Configuration(true);
		Job job=Job.getInstance(conf);

	}

}
