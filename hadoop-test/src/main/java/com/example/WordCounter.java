package com.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCounter {
	
	public static void main1(String [] args) {
		TokenizerMapper mapper = new TokenizerMapper();
		org.apache.hadoop.mapreduce.Mapper.Context context = null;
		try {
			Object key = new Object();
			Text value = new Text("Hello World Bye World");
			mapper.map(key, value, context);
			
			/*key = new Object();
			value = new Text("World");
			mapper.map(key, value, context);
			
			key = new Object();
			value = new Text("Bye");
			mapper.map(key, value, context);
			
			key = new Object();
			value = new Text("World");
			mapper.map(key, value, context);*/
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Map<Text, IntWritable>> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Map<Text, IntWritable> map = new HashMap<>();
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				Text word = new Text();
				IntWritable one = new IntWritable(1);
				word.set(itr.nextToken());
				map.merge(word, one, (v1, v2) -> { v1.set(v1.get() + 1); return new IntWritable(v1.get()); });
			}
			
			for (Text word : map.keySet()) {
				context.write(word, map);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, Map<Text, IntWritable>, Text, IntWritable> {
		private final IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<Map<Text, IntWritable>> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (Map<Text, IntWritable> entry : values) {
				IntWritable val = entry.get(key);
				sum += val.get();
			}
			/*for (IntWritable val : values) {
				sum += val.get();
			}*/
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println("app runs1");
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "wordcount");
		
		job.setJarByClass(WordCounter.class);
		
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.out.println("input file: " + args[0]);
		System.out.println("output file: " + args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}