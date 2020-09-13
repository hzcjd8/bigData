package hadoop02;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TqPartitioner extends  Partitioner<Tq, Text>{
	@Override
	public int getPartition(Tq key, Text value, int numPartitions)  {
		
		
		return key.getYear() % numPartitions;
	}
	

}
