//Read departureDelayCOunt and airport from HBASE table and pass to reducer
//Reference National College of Ireland Postgraduate Diploma in Data Analytics Class Code

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class AirportsOutToHDFSMapper extends 
       TableMapper<Text, IntWritable> {
	
	private Text airportOut= new Text();
	private IntWritable delayCount = new IntWritable();
	
	public void map(ImmutableBytesWritable row, Result columns, Context context)
	       throws IOException, InterruptedException {
	
		String delayCountString = new String (columns.getValue("colfam1".getBytes(), "departureDelayCount".getBytes()));
		delayCount.set(Integer.parseInt(delayCountString));
		
		String airportString = new String(columns.getValue("colfam1".getBytes(), "airport".getBytes()));
		airportOut.set(airportString);
	
		//System.out.println("Key from Mapper is:" + airportOut);
		//System.out.println("Value from Mapper is:" + delayCount);		
		
		context.write(airportOut, delayCount);
	}

}
