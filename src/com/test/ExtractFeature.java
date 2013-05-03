package com.test;

import jAudioFeatureExtractor.DataTypes.RecordingInfo;

import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;

import javax.imageio.ImageIO;

import net.semanticmetadata.lire.imageanalysis.ColorLayout;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.BufferedFSInputStream;
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

import com.test.mfcc12.DataModelForMFCC2D;

public class ExtractFeature {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException{
			JobConf conf = new JobConf();
			String CMD = conf.get("hadoop.local.work.dir") + "/" + "extractFeature.sh";
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
			String hdfsOutputPath = conf.get("fs.default.name") + "/output/dataset/";
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
			FSDataOutputStream fsos2;
			FSDataInputStream fsis;
			
			ColorLayout cl = new ColorLayout();
			
			hdfs.mkdirs(new Path(hdfsKeyFrameFeaturePath));
			hdfs.mkdirs(new Path(hdfsAudioFeaturePath));
			
			// extract the keyFrames' features one by one
			for (Path path : paths) {
				keyFramePath = path.toString();
				name  = keyFramePath.substring(keyFramePath.lastIndexOf('/') + 1);
				keyFrameFileName.set(name);
				
				//extract the keyFrame's  feature
				fsis = hdfs.open(path);
				bi = ImageIO.read(fsis);
				cl.extract(bi);
				
				fsos = hdfs.create(new Path(hdfsKeyFrameFeaturePath + name + ".feature"));
				fsos.writeChars(cl.getStringRepresentation());
				
				// remember to close the stream
				fsis.close();
				fsos.close();
				
				output.collect(keyFrameFileName, new IntWritable(0));
			}
			
			String hdfsAudioFullName = hdfsAudioPath + videoName + ".wav";
			
			// extract audio's mfcc feature
			// TODO try to refactor the following lines
			if (hdfs.exists(new Path(hdfsAudioFullName))) {
				RecordingInfo[] info = new RecordingInfo[1];
				InputStream[] ins = new InputStream[1];
				String[] names = new String[1]; 
				names[0] = hdfsAudioFullName; 
				ins[0] = new BufferedInputStream(hdfs.open(new Path(hdfsAudioFullName)));
				info[0] = new RecordingInfo(hdfsAudioFullName);
				info[0].should_extract_features = true;
				DataModelForMFCC2D dm = new DataModelForMFCC2D("features.xml", null, ins, names);
				
				fsos2 = hdfs.create(new Path(hdfsAudioFeaturePath + videoName + ".wav_feature.xml"));
				fsos = hdfs.create(new Path(hdfsAudioFeaturePath + videoName + ".wav_feature_definition.xml"));
				try {
					dm.featureKey = fsos;
					dm.featureValue = fsos2;
					dm.extract(512, 0, 16000, false, true, true, info, 0);
				} catch (Exception e1) {
					e1.printStackTrace();
				}finally{
					fsos.close();
					fsos2.close();
				}
			}
			
			
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
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
		
		conf.set("mapred.create.symlink", "yes");
		DistributedCache.createSymlink(conf);
		DistributedCache.addCacheFile(new URI("hdfs://localhost:9000" + "/conf/features.xml#features.xml"), conf);

		JobClient.runJob(conf);

	}

}
