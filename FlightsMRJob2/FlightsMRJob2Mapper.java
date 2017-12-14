//AIRLINE and DEPARTURE_DELAY are read from the flightsview table in HBASE
//and passed to the reducer
//Reference National College of Ireland Postgraduate Diploma in Data Analytics Class Code

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FlightsMRJob2Mapper extends 
       TableMapper<Text, IntWritable> {
	
	private Text airlineOut= new Text();
	private IntWritable departureDelay = new IntWritable();

	public void map(ImmutableBytesWritable row, Result columns, Context context)
	       throws IOException, InterruptedException {
		
		String airline = new String(columns.getValue("colfam1".getBytes(), "AIRLINE".getBytes()));
		airlineOut.set(airline);
		String departureDelayString = new String (columns.getValue("colfam1".getBytes(), "DEPARTURE_DELAY".getBytes()));
		departureDelay.set(Integer.parseInt(departureDelayString));
				
		//System.out.println("Key from Mapper is:" + airlineOut);
		//System.out.println("Value from Mapper is:" + departureDelay);		
		
		context.write(airlineOut, departureDelay);
	}

}
