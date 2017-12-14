//The departure delay count grouped by airport is is returned
//Reference National College of Ireland Postgraduate Diploma in Data Analytics Class Code

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;


public class FlightsAirportsMRReducer extends
       TableReducer<Text, IntWritable, ImmutableBytesWritable>{
	
	private int departureDelayCount = 0;
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	       throws IOException, InterruptedException {

		int delayVal =0;
		int numDelays = 0;
		
		for (IntWritable val : values) {
			delayVal = val.get();
		
			if (delayVal >0){    /*Departure delay > 0mins*/
				numDelays++;
			}			
		departureDelayCount = numDelays;

			//System.out.println("Key in Reducer is:" + key);
			//System.out.println("Num of Delays in Reducer is:" + departureDelayCount);
		}
		
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.addColumn("colfam1".getBytes(), "departureDelayCount".getBytes(), Bytes.toBytes(Integer.toString(departureDelayCount)));
		put.addColumn("colfam1".getBytes(), "airport".getBytes(), Bytes.toBytes(key.toString()));
		
		context.write(null, put); 
	}
}
	


