package com.cctc.cite;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;


public class CountReference extends Configured implements Tool {
	
	public static class InvertIndexMapper extends Mapper<Object,Text,Text,IntWritable>{
		private IntWritable citing = new IntWritable(1);
		private Text citied = new Text();
		int counter = 0;
		public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
			counter++;
			String sval = value.toString();
			String str[] = sval.split(",");
			citied.set(str[1]);
			context.write(citied, citing);
			if(counter%100000 == 1)
				System.out.println("##############################"+"this is mapper ####"+counter);
		}
	}
	
	public static class SumCitiedCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable citiedCount = new IntWritable();
		int counter = 0;
		public void reduce(Text key,Iterable<IntWritable> citings,Context context) throws IOException,InterruptedException{
			int  sum = 0;
			for(IntWritable svl : citings){
				sum += svl.get();
				counter++;
			}
			citiedCount.set(sum);
			context.write(key,citiedCount);
			if(counter % 100000 == 1)
				System.out.println("############### this is combiner ######################## "+counter);
		}
	}
	
	public static class SumCitiedReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		private Text citied = new Text();
		private IntWritable citiedSum = new IntWritable();
		
		public void reduce(Text key,Iterable<IntWritable> citingCount,Context context)throws IOException,InterruptedException{
			int sum = 0;
			for(IntWritable count : citingCount){
				sum += count.get();
			}
			citied.set(key);
			citiedSum.set(sum);
			context.write(key, citiedSum);
		}
	}
	


	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	      Job job = new Job(conf, "Count_Reference");
	      job.setJarByClass(CountReference.class);
	      job.setMapperClass(InvertIndexMapper.class);
	      job.setMapOutputKeyClass(Text.class);
	      job.setMapOutputValueClass(IntWritable.class);
	      job.setCombinerClass(SumCitiedCombiner.class);
	      job.setReducerClass(SumCitiedReducer.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(IntWritable.class);
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
	            int ret = ToolRunner.run(new CountReference(), args);
	            System.exit(ret);
			}
		}catch (Exception e){
			e.printStackTrace();
		}
	}
}