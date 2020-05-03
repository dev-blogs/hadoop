package com.example;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCounter {
	public static void main(String [] args) {
		TokenizerMapper mapper = new TokenizerMapper();
		IntSumReducer reducer = new IntSumReducer();
		Text line1 = new Text("1,[a b]");
		Text line2 = new Text("1,[b]");
		Text line3 = new Text("2,[a d e]");
		Text line4 = new Text("3,[a b]");
		try {
			//mapper.map(null, line1, null);
			//mapper.map(null, line2, null);
			//mapper.map(null, line3, null);
			//mapper.map(null, line4, null);
			//System.out.println("---------------------------");
			reducer.reduce(new Text("1"), Arrays.asList(new Text("a")), null);
			reducer.reduce(new Text("1"), Arrays.asList(new Text("b"), new Text("b")), null);
			reducer.reduce(new Text("2"), Arrays.asList(new Text("a")), null);
			reducer.reduce(new Text("2"), Arrays.asList(new Text("d")), null);
			reducer.reduce(new Text("2"), Arrays.asList(new Text("e")), null);
			reducer.reduce(new Text("3"), Arrays.asList(new Text("a")), null);
			reducer.reduce(new Text("3"), Arrays.asList(new Text("b")), null);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String f = getF(value.toString());
			for (String g : getG(value.toString())) {
				//context.write(new Text(f), new Text(g));
				System.out.println(new Text(f) + " " + new Text(g));
			}
		}
		
		private String getF(String text) {
			return text.substring(0, text.indexOf(','));
		}
		
		private String[] getG(String text) {
			return text.substring(text.indexOf('[') + 1, text.indexOf(']')).split(" ");
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private final Map<String, Integer> map = new HashMap<>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Set<String> set = new HashSet<>();
			for (Text val : values) {
				set.add(val.toString());
			}
			for (String s : set) {
				map.merge(s, 1, (v1, v2) -> v1 + v2);
			}
			for (Map.Entry<String, Integer> entry : map.entrySet()) {
				//context.write(new Text(entry.getValue().toString()), new Text(entry.getKey()));
				System.out.println(new Text(entry.getKey()) + " " + new Text(entry.getValue().toString()));
			}
			System.out.println("----------------------");
		}
		
		
	}

	public static void main1(String[] args) throws Exception {
		System.out.println("app runs1");

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "wordcount");

		job.setJarByClass(WordCounter.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.out.println("input file: " + args[0]);
		System.out.println("output file: " + args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
