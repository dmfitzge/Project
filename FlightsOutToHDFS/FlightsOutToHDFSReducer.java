//Store output to HDFS
//Reference National College of Ireland Postgraduate Diploma in Data Analytics Class Code

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;;

public class FlightsOutToHDFSReducer extends
       Reducer<Text, IntWritable, Text, IntWritable>{
	
	public void reduce(Text key, IntWritable values, Context context)
	       throws IOException, InterruptedException {
		
		
		context.write(key, values);
	}
}
