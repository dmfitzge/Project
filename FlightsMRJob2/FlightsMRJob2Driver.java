//This code calculates the average departure delay grouped by Airline
//AIRLINE and DEPARTURE_DELAY are read from the flightsview table in HBASE
//The output is stored in  the flightsOut table in HBASE
//Reference National College of Ireland Postgraduate Diploma in Data Analytics Class Code

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;


public class FlightsMRJob2Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//if (otherArgs.length != 0) {
		//    System.err.println("Usage: HBaseSummarisationDriver2");
		//    System.exit(2);
		//}
		Scan scan = new Scan();
		Job job = Job.getInstance(conf, "Description of MapReduce Job.");
		job.setJarByClass(FlightsMRJob2Driver.class);
		TableMapReduceUtil.initTableMapperJob("flightsView", scan, 
											  FlightsMRJob2Mapper.class, 
				                              Text.class, IntWritable.class, 
				                              job);
		TableMapReduceUtil.initTableReducerJob("flightsOut", FlightsMRJob2Reducer.class, job);
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
