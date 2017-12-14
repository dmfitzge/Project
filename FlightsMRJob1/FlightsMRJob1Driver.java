//This code calculates the average arrival delay grouped by Airline
//AIRLINE and ARRIVAL DELAY are read from the flightsview table in HBASE
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


public class FlightsMRJob1Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();

		Scan scan = new Scan();
		Job job = Job.getInstance(conf, "Description of MapReduce Job.");
		job.setJarByClass(FlightsMRJob1Driver.class);
		TableMapReduceUtil.initTableMapperJob("flightsView", scan, 
											  FlightsMRJob1Mapper.class, 
				                              Text.class, IntWritable.class, 
				                              job);
		TableMapReduceUtil.initTableReducerJob("flightsOut", FlightsMRJob1Reducer.class, job);
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}

