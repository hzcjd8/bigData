package hadoop02;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class TqMapper extends Mapper<LongWritable, Text, Tq, IntWritable>{
  
	Tq mkey=new Tq();
	IntWritable mval=new IntWritable();
	
	
	
   @Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Tq, IntWritable>.Context context)
			throws IOException, InterruptedException {
	   try {
	   String[] strs = StringUtils.split(value.toString(),'\t');
		SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd");
		Date date=sdf.parse(strs[0]);
		Calendar cal=Calendar.getInstance();
		cal.setTime(date);
		mkey.setYear(cal.get(Calendar.YEAR));
		mkey.setMonth(cal.get(Calendar.MONTH)+1);
		mkey.setDay(cal.get(Calendar.DAY_OF_MONTH));
		
		
		int wd=Integer.parseInt(strs[1].substring(0,strs[1].length()-1));
		mkey.setWd(wd);	
		mval.set(wd);
		
		context.write(mkey, mval);
		
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
