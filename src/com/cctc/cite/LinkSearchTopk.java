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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LinkSearchTopk extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1,"Invert_index");
		job1.setJarByClass(SearchTopk.class);
		job1.setMapperClass(SearchTopk.InvertIndexMapper.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);
	    job1.setInputFormatClass(KeyValueTextInputFormat.class);
	    job1.setOutputFormatClass(TextOutputFormat.class);
	    conf1.set("key.value.separator.in.input.line"," ");
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
	    job1.waitForCompletion(true);
	    
	    Configuration conf2 = new Configuration();
	    Job job2 = new Job(conf2,"heap_sort");
	    job2.setJarByClass(SearchTopk.class);
	    job2.setMapperClass(SearchTopk.MaxHeapMapper.class);
	    job2.setMapOutputKeyClass(IntWritable.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setReducerClass(SearchTopk.MergeReducer.class);
	    job2.setOutputKeyClass(IntWritable.class);
	    job2.setOutputValueClass(Text.class);
	    job2.setInputFormatClass(KeyValueTextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	    conf2.set("key.value.separator.in.input.line"," ");
	    FileInputFormat.addInputPath(job2, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	    conf2.set("heapsize", args[3]);
	    job2.setNumReduceTasks(1);
	    job2.waitForCompletion(true);
	   
		return 0;
	}
	
	public static void main(String args[]){
		try{
			if(args.length != 4){
		        System.err.println("Usage: LinkSearchTopk <in1> <out1/in2> <out2> <heapsize>");
		        System.exit(2);
			}else{
	            int ret = ToolRunner.run(new LinkSearchTopk(), args);
	            System.exit(ret);
			}
		}catch (Exception e){
			e.printStackTrace();
		}
	}
}
