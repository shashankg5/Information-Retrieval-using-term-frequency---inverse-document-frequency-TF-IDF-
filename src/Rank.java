//Shashank Gupta sgupta27@uncc.edu
package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class Rank extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( Rank.class);
	
	public static void main( String[] args) throws  Exception {
		
		//output from Search.java is used as input to Rank as passed in args
		int res  = ToolRunner.run(new Rank(), args);
		System .exit(res);

	}
	public int run( String[] args) throws  Exception {
		Job job = Job.getInstance(getConf());

		job.setJarByClass(this.getClass());
		job.setJobName(" Ranker ");
		job.setMapperClass(RankMap.class);
		job.setReducerClass(RankReducer.class);
		job.setSortComparatorClass(LongWritable.DecreasingComparator.class); //Comparator to keep the rank in descending
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}

	


	public static class RankMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
							throws IOException, InterruptedException {
			String doc;
			double rank;

			doc   = lineText.toString().split("\t")[0];	//splitting
			rank = Double.parseDouble(lineText.toString().split("\t")[1]);
			
			context.write(new DoubleWritable(rank), new Text(doc));
		}
	}

	public static class RankReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

		@Override 
		public void reduce(DoubleWritable rank, Iterable<Text> docs, Context context)
								throws IOException, InterruptedException {
			for (Text doc : docs) {
				context.write(doc, rank);	//simple iteration
			}
		}
	}
}
