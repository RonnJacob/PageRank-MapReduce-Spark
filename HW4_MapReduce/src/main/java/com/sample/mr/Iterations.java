package com.sample.mr;

import java.net.URI;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
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

public class Iterations extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(Iterations.class);

	public static enum PAGE_MASS {
		sdm_mass_transfer
	}


	public static final float alpha = 0.85f;
	public static final int k = 100;

	public static class PageRankMapper1 extends Mapper<Object, Text, Text, Text> {

		private Text v1 = new Text();
		private Text v1_no = new Text();
		private Text neighbour = new Text();
		private Text neighbour_rank = new Text();



		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			//Retrieving
			final StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
			String valueKey = itr.nextToken();
			String valueVal = itr.nextToken();
			String[] adjList = valueVal.split(";");

			// Retrieving the list of neighbours
			String[] neighbours=adjList[0].split("#");

			//Retrieving the rank of the vertex
			String rank = adjList[1];


			v1.set(valueKey);
			v1_no.set(valueVal);
			context.write(v1,v1_no);

			// Rank distributed among all the neighbours
			Float updated_rank = Float.parseFloat(rank)/(neighbours.length);

			// Updating the ranks of the neighbour split by the number of neighbours
			for(String s:neighbours) {
				if (!s.isEmpty()) {
					neighbour.set(s);
					neighbour_rank.set(updated_rank.toString());
					context.write(neighbour, neighbour_rank);
				}
			}

		}
	}

	public static class PageRankReducer1 extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

			List<String> valuesList = new ArrayList<>();
			String[] neighbours;
			String vertexList="";

			// Check if the vertex encountered is the single dummy vertex
			if(Integer.parseInt(key.toString())==0){
				double total_sdm_mass = 0.0;
				for (Text node : values) {
					total_sdm_mass+=Double.parseDouble(node.toString());
				}
				context.getCounter(PAGE_MASS.sdm_mass_transfer).setValue((long)((total_sdm_mass/(k*k))*(k*k)));
			}

			//Otherwise we would calculate the page rank.
			if(Integer.parseInt(key.toString())!=0) {
				for (Text node : values) {
					valuesList.add(node.toString());
					if (node.toString().contains(";")) {
						neighbours = node.toString().split(";");
						vertexList = neighbours[0];
					}

				}
				if(valuesList.size()==1)
				{
					Text valueOut = new Text(vertexList+";"+"0");
					logger.info("Page rank value=="+0+" for---"+key);
					context.write(key,valueOut);
				}
				else{
					float sumPR =0.0f;
					for(String s: valuesList)
					{
						if(!s.contains(";")) {
							sumPR += Float.parseFloat(s);
						}

					}

					//Calculating page rank for the
					float PR = (alpha*sumPR) + ((1-alpha)/(k*k));
					Text valueOut = new Text(vertexList+";"+PR);
					context.write(key,valueOut);

				}
			}
		}
	}

	public static class PageRankMapper2 extends Mapper<Object, Text, Text, Text> {

		private Text v1 = new Text();
		private Text v1_no = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			final StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
			String valueKey = itr.nextToken();
			String valueVal = itr.nextToken();
			v1.set(valueKey);
			v1_no.set(valueVal);
			context.write(v1,v1_no);
		}
	}

	public static class PageRankReducer2 extends Reducer<Text, Text, Text, Text> {
		public double convertedMass;
		protected void setup(Context context) throws IOException, InterruptedException {
			convertedMass = Double.parseDouble(context.getConfiguration().get("PageRankMass.Val"))/k*k;
		}

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			List<String> valuesList = new ArrayList<>();
			for (Text node : values) {
				valuesList.add(node.toString());
			}

			String[] neighbor_rank = valuesList.get(0).split(";");
			Double updated_pagerank = Double.parseDouble(neighbor_rank[1])+(alpha*convertedMass);
			Text valueOut = new Text(neighbor_rank[0]+";"+updated_pagerank.toString());
			context.write(key,valueOut);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		String intermeOut = "s3://my-projectbucket/outputInter/inter";

		//Job 1
		final Job job = Job.getInstance(conf, "Page Rank Calculation");
		job.setJarByClass(Iterations.class);
		final Configuration jobConf1 = job.getConfiguration();
		jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");
		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(job, new Path(args[0]));
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 50000);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(intermeOut));


		job.setMapperClass(PageRankMapper1.class);
		job.setReducerClass(PageRankReducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true)){
            System.exit(1);
        }

		final Configuration conf1 = getConf();
		// Job2 has a Mapper and a Reducer
		final Job job2 =  Job.getInstance(conf1, "Page rank evaluation");
		job2.setJarByClass(Iterations.class);
		final Configuration jobConf2 = job2.getConfiguration();
		jobConf2.set("mapreduce.output.textoutputformat.separator2", "\t");
		Counter st 	= job.getCounters().findCounter(PAGE_MASS.sdm_mass_transfer);
		Long dummyMassVal= st.getValue();
		String stName = st.getDisplayName();
		jobConf2.set("PageRankMass.Val",String.valueOf(dummyMassVal));
		job2.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(job2, new Path(intermeOut));
		job2.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 50000);
		LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		job2.setMapperClass(PageRankMapper2.class);
		job2.setReducerClass(PageRankReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		FileSystem fs1 = FileSystem.get(new URI("s3://my-projectbucket"),conf2);
		fs1.delete(new Path(intermeOut));

		return 0;

	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir> <mass>");
		}
		try {
			ToolRunner.run(new Iterations(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}