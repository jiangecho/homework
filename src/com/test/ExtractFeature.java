package com.test;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class ExtractFeature {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static String cmd = "extractKeyFrame.sh ";
		private final static String RESULT_DIR = "output/dataset/";

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException{
			String videoLocalFullName = value.toString();
			String videoLocalPath = videoLocalFullName.substring(0, videoLocalFullName.lastIndexOf('/') + 1);
			String videoName = videoLocalFullName.substring(videoLocalFullName.lastIndexOf('/') + 1);
			
			try {
				Process process = Runtime.getRuntime().exec(videoLocalPath + cmd + videoLocalFullName);
				process.waitFor();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			
			Text keyFrameFileName = new Text();

			JobConf conf = new JobConf();
			String hdfsDefaultPath = conf.get("fs.default.name");
			FileSystem hdfs = FileSystem.get(URI.create(hdfsDefaultPath), conf);
			FileStatus[] fileStatus = hdfs.listStatus(new Path(RESULT_DIR + videoName + "/keyFrame"));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			
			String keyFramePath;
			String name;
			for (Path path : paths) {
				keyFramePath = path.toString();
				name  = keyFramePath.substring(keyFramePath.lastIndexOf('/') + 1);
				keyFrameFileName.set(name);
				output.collect(keyFrameFileName, new IntWritable(0));
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			output.collect(key, new IntWritable(1));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(ExtractFeature.class);
		conf.setJobName("extractFeature");
		conf.setJarByClass(ExtractFeature.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);

	}

}
