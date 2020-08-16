package com.sample.mr;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class PageRank extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(PageRank.class);
	public static int response;
	public static double pr_init = (double)1/(double)(100*100);

	public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {

		private Text v1 = new Text();
		private Text v2 = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			// Input file is of the format
			// 1,2
			// 2,3
			// 3,0
			// 4,5
			// 5,6
			// 6,0
			final StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			String src_vtx = itr.nextToken();
			String dest_vtx = itr.nextToken();
			v1.set(src_vtx);
			v2.set(dest_vtx);
			context.write(v1, v2);
		}
	}

	public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			Text nextPath = new Text();
			Text nextValue = new Text();
			nextPath.set(key);
			String neighbour = "";
			for (Text node : values) {
				neighbour += node + "#";
			}
			nextValue.set(neighbour.substring(0,neighbour.length()-1)+";"+pr_init);
			context.write(nextPath,nextValue);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		// Job1 has a Mapper and a Reducer
		final Job job1 = Job.getInstance(conf, "Page Rank Iterations");
		job1.setJarByClass(PageRank.class);
		final Configuration jobConf1 = job1.getConfiguration();
		jobConf1.set("k_value",args[2]);
		jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");
		job1.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(job1, new Path(args[0]));
		//Making sure that we have 20 mappers by splitting 100*100 over 20 mappers which
		// would mean that we are splitting it over 20 mappers.
		job1.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500 );
		LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.setMapperClass(PageRankMapper.class);
		job1.setReducerClass(PageRankReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		return job1.waitForCompletion(true)?0:1;
	}

	public static void main(final String[] args) {
		if (args.length != 4) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir> <iterations> <k_value>");
		}
		try {
			// Defining output paths for the files.
			String prevIterationOutput = "";
			String iterationOutput ="";
			String iterations = "s3://my-projectbucket/iterations";
			String pageRankCalc = "s3://my-projectbucket/final";
//			Configuration confFile = new Configuration();

			// Here we would generate the initial page ranks for each of the nodes initialized to 1/(k^2)
			String initialIterationInput = "s3://my-projectbucket/initial";
			response = ToolRunner.run(new PageRank(), new String[]{args[0], initialIterationInput, args[3]});


//			ToolRunner.run(new Iterations(), new String[]{initialIterationInput, iterations + "/iteration1"});
//			FileSystem fs = FileSystem.get(new URI("output"),confFile);
//			fs.delete(new Path(initialIterationInput));


			logger.info("<------------Running the PageRank program for "+args[3] + " iterations------>");
			for (int i = 1; i <= 10; i++) {
				if (i==1){
					prevIterationOutput = initialIterationInput;
					iterationOutput = iterations + "/iteration1";
				}
				else{
					prevIterationOutput = iterations + "/iteration" + (i - 1);
					iterationOutput = iterations + "/iteration" + i;
				}

				ToolRunner.run(new Iterations(), new String[]{prevIterationOutput, iterationOutput});
				Configuration confFile = new Configuration();
				FileSystem fs = FileSystem.get(new URI("s3://my-projectbucket"),confFile);
				fs.delete(new Path(prevIterationOutput));

			}
			logger.info("input path is====="+iterationOutput);
            ToolRunner.run(new FinalPageRank(), new String[]{iterationOutput,pageRankCalc});

		} catch (final Exception e) {
			logger.error("", e);
		}

	}
}