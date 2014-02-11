package com.cctc.cite;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cctc.cite.CountReferTimes.CountReferTimeCombiner;
import com.cctc.cite.CountReferTimes.CountReferTimeReducer;
import com.cctc.cite.CountReference.InvertIndexMapper;
import com.cctc.cite.CountReference.SumCitiedCombiner;
import com.cctc.cite.CountReference.SumCitiedReducer;


public class LinkMRtask extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration jobconf1 = new Configuration();
		String[] otherArgs1 = new GenericOptionsParser(jobconf1, args).getRemainingArgs();
		Job job1 = new Job(jobconf1, "Count_Reference");
		job1.setJarByClass(CountReference.class);
	    job1.setMapperClass(InvertIndexMapper.class);
	    job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(IntWritable.class);
	    job1.setCombinerClass(SumCitiedCombiner.class);
	    job1.setReducerClass(SumCitiedReducer.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job1, new Path(otherArgs1[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(otherArgs1[1]));
	    job1.waitForCompletion(true);
	    
	    Configuration jobconf2 = new Configuration();
		String[] otherArgs2 = new GenericOptionsParser(jobconf1, args).getRemainingArgs();
		Job job2 = new Job(jobconf2, "Count_Reference");
	    job2.setJarByClass(CountReferTimes.class);
	    job2.setMapperClass(CountReferTimes.InvertIndexMapper.class);
	    job2.setMapOutputKeyClass(IntWritable.class);
	    job2.setMapOutputValueClass(IntWritable.class);
	    job2.setCombinerClass(CountReferTimeCombiner.class);
	    job2.setReducerClass(CountReferTimeReducer.class);
	    job2.setOutputKeyClass(IntWritable.class);
	    job2.setOutputValueClass(IntWritable.class);
	    job2.setInputFormatClass(KeyValueTextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	    jobconf2.set("key.value.separator.in.input.line"," ");
	    FileInputFormat.addInputPath(job2, new Path(otherArgs2[1]));
	    FileOutputFormat.setOutputPath(job2, new Path(otherArgs2[2]));
	    return (job2.waitForCompletion(true) ? 0 : 1);
	}
	
/*	public static void main(String args[]){
		try{
			if(args.length != 3){
		        System.err.println("Usage: CountReference <in> <tmp> <out>");
		        System.exit(2);
			}else{
	            int ret = ToolRunner.run(new LinkMRtask(), args);
	            System.exit(ret);
			}
		}catch (Exception e){
			e.printStackTrace();
		}
	}
*/
}
