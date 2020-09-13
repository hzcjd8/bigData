package hadoop03;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class FoFMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
  
	Text mkey=new Text();
	IntWritable mval=new IntWritable();
	
	
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
	
		//tom       hello hadoop cat 
		String[] strs = StringUtils.split(value.toString(), ' ');
		
		String user=strs[0];
		String user01=null;
		for(int i=1;i<strs.length;i++){
			
			
			mkey.set(fof(strs[0],strs[i]));  
			mval.set(0); 
			
			context.write(mkey, mval);  
			
			for (int j = i+1; j < strs.length; j++) {
				Thread.sleep(context.getConfiguration().getInt("sleep", 0));
				mkey.set(fof(strs[i],strs[j]));  
				mval.set(1);  
				//key  value
				//tom:hello  0
				//   hello:hadoop 1
				//   hello:cat  1
				//tom:hadoop 0
				//    hadoop:cat 1
				//tom:cat  0 
				context.write(mkey, mval); //value =0直接关系 value=1间接关系
				
				
			}
		}
		
		


}
	
	
	public static String fof(String str1  , String str2){
		
		
		if(str1.compareTo(str2) > 0){
			//hello,hadoop
			return str2+":"+str1;
			//hadoop:hello
		}
			
		
		//hadoop,hello
		return str1+":"+str2;
		//hadoop:hello
		
		
		
	}
}	
