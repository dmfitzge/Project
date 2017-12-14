//The average arrival delay grouped by airport is is returned
//Reference National College of Ireland Postgraduate Diploma in Data Analytics Class Code

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;


public class FlightsMRJob2Reducer extends
       TableReducer<Text, IntWritable, ImmutableBytesWritable>{
	
	private int count = 0;
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	       throws IOException, InterruptedException {

		int sum = 0;
		int average = 0;
		
		for (IntWritable val : values) {
			sum += val.get();
			count++;
			
		}
		
		average = (sum/count);
		count=0;
		
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.addColumn("colfam1".getBytes(), "avgDepartureDelay".getBytes(), Bytes.toBytes(Integer.toString(average)));
		
		//System.out.println("Key from Reducer is:" + put);
		//System.out.println("Value from Mapper is:" + put);		
		
		context.write(null, put); 
		
	}
	

}

