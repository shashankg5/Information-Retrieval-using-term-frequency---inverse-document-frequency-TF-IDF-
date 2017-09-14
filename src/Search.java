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


public class Search extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( Search.class);

	public static void main( String[] args) throws  Exception {
		
		//providing the output of TFIDF as input arg
		int res  = ToolRunner.run(new Search(), args);
		
		System .exit(res);

	}
	public int run( String[] args) throws  Exception {

		Configuration conf = getConf();

		
		conf.set("Query", args[2]);

		Job job = Job .getInstance(conf, "job");
		job.setJarByClass(Search.class);

		job.setMapperClass(MapSearch.class);
		job.setReducerClass(ReduceSearch.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	


	public static class MapSearch extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
		private Text word  = new Text();

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String q;
			//getting the query
			Configuration conf = context.getConfiguration();
			q  = conf.get("Query").toLowerCase();

			String line  = lineText.toString();
			Text currentWord  = new Text();

			String[] split1 = line.split("\\t");
			String[] split2 = split1[0].split("#####");

			//Getting word
			String word = split2[0];
			//Getting filename
			String fileName = split2[1];
			double idf = Double.parseDouble(split1[1]);

			//creating search strings
			String[] searchStr = q.split(" ");

			for (String str: searchStr) {
				if(word.equals(str.toLowerCase())) {
					context.write(new Text(fileName), new DoubleWritable(idf));

				}
			}
		}
	}

	public static class ReduceSearch extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {

		@Override 
		public void reduce( Text word,  Iterable<DoubleWritable> files,  Context context)
				throws IOException,  InterruptedException {
			double score = 0;
			//adding to the file
			for ( DoubleWritable file  : files) {
				score = score+file.get();
			} 	  

			context.write(word,  new DoubleWritable(score));
		}
	}
}
