package com.test;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import javax.imageio.ImageIO;

import net.semanticmetadata.lire.imageanalysis.ColorLayout;

import org.apache.commons.math3.exception.NullArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
		private final static String CMD = "/home/xianer/workspace/MultiMedia/extractFeature.sh";
		private final static String RESULT_DIR = "output/dataset/";

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException{
			String videoLocalFullName = value.toString();
			String videoName = videoLocalFullName.substring(videoLocalFullName.lastIndexOf('/') + 1);
			String[] cmds = new String[] {CMD,  videoLocalFullName};
			
			System.out.println("start extract ");
			try {
				//call a shell script to extract the keyFrames, and then upload the keyFrames to hdfs
				Process process = Runtime.getRuntime().exec(cmds);
				process.waitFor();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			System.out.println("extract over");
			
			Text keyFrameFileName = new Text();
			JobConf conf = new JobConf();
			String hdfsOutputPath = conf.get("fs.default.name") + "/user/xianer/output/dataset/";
			//String hdfsOutputPath = "hdfs://localhost:9000/user/xianer" + "/output/dataset/";
			String hdfsKeyFramePath = hdfsOutputPath + videoName +  "/keyFrame/";
			String hdfsKeyFrameFeaturePath = hdfsOutputPath + videoName + "/keyFrameFeature/";
			String hdfsAudioPath = hdfsOutputPath + videoName + "/audio/";
			String hdfsAudioFeaturePath = hdfsOutputPath + videoName + "/audioFeature/";
			
			System.out.println(hdfsKeyFramePath);
			System.out.println(hdfsKeyFrameFeaturePath);
			
			FileSystem hdfs = FileSystem.get(URI.create(hdfsOutputPath), conf);
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsKeyFramePath));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			
			String keyFramePath;
			String name;
			BufferedImage bi;
			FSDataOutputStream fsos;
			FSDataInputStream is;
			ColorLayout cl = new ColorLayout();
			
			hdfs.mkdirs(new Path(hdfsKeyFrameFeaturePath));
			
			for (Path path : paths) {
				keyFramePath = path.toString();
				name  = keyFramePath.substring(keyFramePath.lastIndexOf('/') + 1);
				keyFrameFileName.set(name);
				
				//extract the keyFrame's  feature
				is = hdfs.open(path);
				bi = ImageIO.read(is);
				cl.extract(bi);
				
				fsos = hdfs.create(new Path(hdfsKeyFrameFeaturePath + name + ".feature"));
				fsos.writeChars(cl.getStringRepresentation());
				
				// remember to close the stream
				is.close();
				fsos.close();
				
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
