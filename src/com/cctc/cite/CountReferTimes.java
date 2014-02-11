package com.cctc.cite;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class CountReferTimes {
	public static class InvertIndexMapper extends Mapper<Text,Text,IntWritable,IntWritable>{
		private IntWritable referTimes = new IntWritable();
		private IntWritable referIndex = new IntWritable(1);
		
		public void map(Text key,Text value,Context context) throws IOException,InterruptedException{
			//String[] str = value.toString().split(" ");
			String str = value.toString();
			referTimes.set(Integer.parseInt(str));
			context.write(referTimes, referIndex);
		}
	}
	
	
	public static class CountReferTimeCombiner extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		
		private IntWritable referTimeNum = new IntWritable();
		public void reduce(IntWritable key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
			int tempsum = 0;
			for(IntWritable index : value){
				tempsum+=index.get();
			}
			referTimeNum.set(tempsum);
			context.write(key, referTimeNum);
		}
	}
	
	public static class CountReferTimeReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		private IntWritable sum = new IntWritable();
		
		public void reduce(IntWritable key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
		    int reducesum = 0;
			for(IntWritable iter : value){
				reducesum += iter.get();
			}
			sum.set(reducesum);
			context.write(key, sum);
		}
	}
/*	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	      Job job = new Job(conf, "Count_Reference");
	      job.setJarByClass(CountReferTimes.class);
	      job.setMapperClass(InvertIndexMapper.class);
	      job.setMapOutputKeyClass(IntWritable.class);
	      job.setMapOutputValueClass(IntWritable.class);
	      job.setCombinerClass(CountReferTimeCombiner.class);
	      job.setReducerClass(CountReferTimeReducer.class);
	      job.setOutputKeyClass(IntWritable.class);
	      job.setOutputValueClass(IntWritable.class);
	      job.setInputFormatClass(KeyValueTextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);
	      conf.set("key.value.separator.in.input.line"," ");
	      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	      FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	      return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String args[]){
		try{
			if(args.length != 2){
		        System.err.println("Usage: CountReference <in> <out>");
		        System.exit(2);
			}else{
	            int ret = ToolRunner.run(new CountReferTimes(), args);
	            System.exit(ret);
			}
		}catch (Exception e){
			e.printStackTrace();
		}
	}
*/
}