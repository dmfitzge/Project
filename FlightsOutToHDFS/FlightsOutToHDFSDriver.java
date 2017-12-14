//This code reads avgArrivalDelay, avgDepartureDelay and airline from HBASE table flightsOut
//and writes them to /user/hduser/flightsOutToHDFS/output in HDFS
//Reference National College of Ireland Postgraduate Diploma in Data Analytics Class Code

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class FlightsOutToHDFSDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("fs.defaultFS", "hdfs://localhost:54310");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Scan scan = new Scan();
		Job job = Job.getInstance(conf, "C.");
		job.setJarByClass(FlightsOutToHDFSDriver.class);
		TableMapReduceUtil.initTableMapperJob("flightsOut", scan, 
											  FlightsOutToHDFSMapper.class, 
				                              Text.class, IntWritable.class, 
				                              job);
		job.setReducerClass(FlightsOutToHDFSReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path("/user/hduser/FlightsOutToHDFS/output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
