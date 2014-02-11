package com.cctc.cite;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import com.cctc.cite.ArrayPriorityHeap.PairEntry;




public class SearchTopk{
	
	//这个预处理mapper类完成数据的倒置，因为要以数据被引用的次数作为键统计topk。
	public static class InvertIndexMapper extends Mapper<Text,Text,IntWritable,Text>{
		private IntWritable citedTimes = new IntWritable(0);
		public void map(Text key,Text value,Context context) throws IOException,InterruptedException{
			int number = Integer.parseInt(value.toString());
			citedTimes.set(number);
			context.write(citedTimes, key);
		}
	}
	
	
	//优先队列mapper类，构建大小为k的优先堆，最后输出topk个索引。
	public static class MaxHeapMapper extends Mapper<Text,Text,IntWritable,Text>{
		
		int heapsize = 0;
		ArrayPriorityHeap<IntWritable,Text> arrheap = null;
		int i = 0;
		protected void setup(Context context
                  ) throws IOException, InterruptedException {
			//heapsize = Integer.parseInt(context.getConfiguration().get("heapsize"));
			System.out.print("######################################  ");
			System.out.println(context.getConfiguration().get("heapsize"));
			heapsize = 100;
			//从命令行获取参数k，初始化heapsize；
			arrheap = new ArrayPriorityHeap<IntWritable,Text>(heapsize,IntWritable.class,Text.class);
		 }
		public void map(Text key,Text value,Context context){
			IntWritable keyint = new IntWritable(Integer.parseInt(key.toString()));
			Text val = new Text(value.toString());
/*			if(keyint.get()>1){
				System.out.print(" ");
			}*/
			if(!arrheap.insertHeap(keyint,val)){
				arrheap.deHeap();
				arrheap.insertHeap(keyint,val);
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			arrheap.deHeap();
			Iterator iter = arrheap.iterator();
			PairEntry<IntWritable, Text> entry = null;
/*			for(int i = 1;i<=heapsize;i++){
				entry = arrheap.arrayheap[i];
				System.out.print(" "+entry.getKey()+ " ");
			}*/
			while(iter.hasNext()){
				entry = (PairEntry<IntWritable, Text>) iter.next();
				context.write(entry.getKey(), entry.getValue());
				System.out.print(" "+entry.getKey()+"/"+entry.getValue()+ " ");
			}
		}
			
	}
	
	//合并多个map输出的堆
	public static class MergeReducer extends Reducer<IntWritable,Text,IntWritable,Text>{
		int heapsize;
		ArrayPriorityHeap<IntWritable,Text> arrheap2 = null;
		
		public void setup(Context context
                  ) throws IOException, InterruptedException {
			heapsize = 100;//Integer.parseInt(context.getConfiguration().get("heapsize"));
			arrheap2 = new ArrayPriorityHeap<IntWritable,Text>(heapsize,IntWritable.class,Text.class);
		}
		//初始化heapsize
		
		public void reduce(IntWritable key,Iterable<Text> values,Context context){
			IntWritable keyint = new IntWritable(key.get());
			for(Text v :values){
				Text val = new Text(v.toString());
				if(!arrheap2.insertHeap(keyint, val)){
					arrheap2.deHeap();
					arrheap2.insertHeap(keyint, val);
				}
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			Iterator iter = arrheap2.iterator();
			Map.Entry<IntWritable, Text> entry2 = null;
			while(iter.hasNext()){
				entry2 = (Entry<IntWritable, Text>) iter.next();
				context.write(entry2.getKey(), entry2.getValue());
			}
		}
	}

}
