//Shashank Gupta sgupta27@uncc.edu

package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.lang.*;
import java.util.HashMap;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TFIDF.class);
	private static final String INTER_PATH = "intermediate";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TFIDF(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), "job1");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(INTER_PATH));
		job.setMapperClass(MapTermF.class);
		job.setReducerClass(ReduceTermF.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.waitForCompletion(true);

		Job job2 = Job.getInstance(getConf(), "job2");
		job2.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job2, INTER_PATH);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setMapperClass(MapTFIDF.class);
		job2.setReducerClass(ReduceTFIDF.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		return job2.waitForCompletion(true) ? 0 : 1;

	}

	public static class MapTermF extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			int inputLength = (int) context.getConfiguration().getLong("mapred.map.tasks", 1);
			String line = lineText.toString();
			Text currentWord = new Text();

			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				//adding the word + filename + length
				currentWord = new Text(word.toLowerCase()+ "#####"+ ((FileSplit) context.getInputSplit()).getPath().getName() + "#####"+ String.valueOf(inputLength));
				context.write(currentWord, one);
			}
		}
	}

	public static class ReduceTermF extends
			Reducer<Text, IntWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> count, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : count) {
				sum += count.get();
			}
			double logFreq = 0;
			 if(sum <= 0) {	//if equal to 0
				 logFreq = 0;
			 }
			 else {
				 logFreq = 1 + Math.log10(sum);
			 }
			 context.write(word,  new DoubleWritable(logFreq));
			}
	}
	
	public static class MapTFIDF extends Mapper<Text, Text, Text, Text> {

		private Text word = new Text();

		public void map(Text word, Text count, Context context)
				throws IOException, InterruptedException {

			String line = word.toString();
			String[] str_array = line.split("#####");
			if (str_array.length == 3)
				context.write(new Text(str_array[0]), new Text(str_array[1] + "=" + count.toString() + "=" + str_array[2]));
		}
	}
	public static class ReduceTFIDF extends Reducer<Text, Text, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<Text> count, Context context)
				throws IOException, InterruptedException {
			int inputLength = 0;
			double count = 0.0;
			
			// HashMap is use to store search list
			HashMap<String, String> search_map = new HashMap<String, String>();
			
			for (Text entry : count) {
				count++;
				String[] str_array = entry.toString().split("=");
				if (str_array.length >= 2) {
					search_map.put(str_array[0], str_array[1]);
					inputLength = Integer.parseInt(str_array[2]);

				}

			}
			double dd;
			dd = Math.log10(1 + (inputLength / count));
			
			for (String key : search_map.keySet())
				context.write(new Text(word.toString() + "#####" + key),new DoubleWritable(dd*Double.parseDouble(search_map.get(key))));

		}
	}
}

