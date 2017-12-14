//Read avgDepartureDelay,  avgArrivalDelay and airline from HBASE table and pass to reducer
//Reference National College of Ireland Postgraduate Diploma in Data Analytics Class Code

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;

public class FlightsOutToHDFSMapper extends 
		TableMapper<Text, Text> {
	
	private Text airlineOut = new Text();
	private Text avgArrivalDelayOut = new Text();
	private Text avgDepartureDelayOut = new Text();
	
	public void map(ImmutableBytesWritable row, Result columns, Context context)
	       throws IOException, InterruptedException {
		
		String airlineString = new String (columns.getValue("colfam1".getBytes(), "AIRLINE".getBytes()));
		airlineOut.set(airlineString);
		
		String avgArrivalDelayString = new String(columns.getValue("colfam1".getBytes(), "avgArrivalDelay".getBytes()));
		avgArrivalDelayOut.set(avgArrivalDelayString);
		
		String avgDepartureDelayString = new String(columns.getValue("colfam1".getBytes(), "avgDepartureDelay".getBytes()));
		avgDepartureDelayOut.set(avgDepartureDelayString);
		
		String Output = avgArrivalDelayOut + "\t" + avgDepartureDelayOut;
	
		
		//System.out.println("Key from Mapper is:" + airlineOut);
		//System.out.println("Value from Mapper is:" + Output);		
		
		context.write(airlineOut, new Text(Output));
	}

}

