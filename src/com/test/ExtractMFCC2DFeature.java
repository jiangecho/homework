package com.test;
import jAudioFeatureExtractor.DataTypes.RecordingInfo;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


public class ExtractMFCC2DFeature {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		RecordingInfo[] info = new RecordingInfo[1];
		info[0] = new RecordingInfo("test.wav");
		info[0].should_extract_features = true;
		InputStream[] ins = new InputStream[1];
		try {
			//ins[0] = new FileInputStream(new File("test.wav"));
			ins[0] = new BufferedInputStream(new FileInputStream(new File("test.wav")));
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		String[] names = new String[1];
		names[0] = "test.wav";
		
		DataModelForMFCC2D dm = new DataModelForMFCC2D("features.xml", null, ins, names);
		File definition = new File("feature_definition.xml");
		File feature = new File("feature.xml");
		OutputStream definitionOs = null; 
		OutputStream featureOs = null;
		try {
			definitionOs = new FileOutputStream(definition);
			featureOs = new FileOutputStream(feature);
			dm.featureKey = definitionOs;
			dm.featureValue = featureOs;
			dm.extract(512, 0, 16000, false, true, true, info, 0);
		} catch (Exception e1) {
			e1.printStackTrace();
		}finally{
			try {
				if (definitionOs != null) {
					definitionOs.close();
				}
				if (featureOs != null) {
					featureOs.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		ins[0].close();
	}

}
