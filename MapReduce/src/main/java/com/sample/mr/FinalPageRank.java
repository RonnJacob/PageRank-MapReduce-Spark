package com.sample.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;


public class FinalPageRank extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(FinalPageRank.class);

	public static class TotalMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {


			final StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
			String valueKey = itr.nextToken();
			// We would retrieve each of the page ranks and emit it
			String[] adjList =  itr.nextToken().split(";");
			context.write(new Text("PR"),new Text(adjList[1]));

		}
	}

	public static class TotalReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

			// We would sum up all of the ranks.
			double final_rank =0.0;
			for(Text node: values)
			{
				final_rank+=Double.parseDouble(node.toString());
			}
			context.write(new Text("Page Rank"),new Text(String.valueOf(final_rank)));
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();

		// Job1 has a Mapper and a Reducer
		final Job job1 = Job.getInstance(conf, "Reduce Side Join1");
		job1.setJarByClass(FinalPageRank.class);
		final Configuration jobConf1 = job1.getConfiguration();
		jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");
		job1.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(job1, new Path(args[0]));
		job1.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 50000);
		LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));


		job1.setMapperClass(TotalMapper.class);
		job1.setReducerClass(TotalReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		return job1.waitForCompletion(true)?0:1;
	}



	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			int status = ToolRunner.run(new FinalPageRank(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}