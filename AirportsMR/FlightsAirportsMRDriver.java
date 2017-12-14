
//This code calculates the number of departure delays grouped by Airport
//ORIGIN_AIRPORT and DEPARTURE DELAY are read from the flightsview table in HBASE
//The output is stored in  the airportsOut table in HBASE
//Reference National College of Ireland Postgraduate Diploma in Data Analytics Class Code

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;


public class FlightsAirportsMRDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Scan scan = new Scan();
		Job job = Job.getInstance(conf, "Number of Delays Grouped by Airport.");
		job.setJarByClass(FlightsAirportsMRDriver.class);
		TableMapReduceUtil.initTableMapperJob("flightsView", scan, 
											  FlightsAirportsMRMapper.class, 
				                              Text.class, IntWritable.class, 
				                              job);
		TableMapReduceUtil.initTableReducerJob("airportsOut", FlightsAirportsMRReducer.class, job);
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}

