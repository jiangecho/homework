package com.test;
import java.io.File;
import java.io.InputStream;

import com.sun.org.apache.xerces.internal.parsers.XMLDocumentParser;

import jAudioFeatureExtractor.DataModel;
import jAudioFeatureExtractor.ModelListener;
import jAudioFeatureExtractor.Aggregators.Aggregator;
import jAudioFeatureExtractor.Aggregators.AggregatorContainer;
import jAudioFeatureExtractor.DataTypes.RecordingInfo;

/**
 * All components that are not tightly tied to GUI. Used by console interface as
 * well as the GUI interface.
 * 
 * @author Daniel McEnnis
 */
public class DataModelForMFCC2D extends DataModel {

	private InputStream[] ins;
	private String[] name;
	public DataModelForMFCC2D(String featureXMLLocation, ModelListener ml, InputStream[] ins, String[] names) {
		super(featureXMLLocation, ml);
		this.ins = ins;
		this.name = names;
	}

	@Override
	public void extract(int windowSize, double windowOverlap,
			double samplingRate, boolean normalise, boolean perWindowStats,
			boolean overallStats, RecordingInfo[] info, int arff)
			throws Exception {

		// Get the control parameters
		boolean save_features_for_each_window = perWindowStats;
		boolean save_overall_recording_features = overallStats;
		int window_size = windowSize;
		double window_overlap = windowOverlap;
		double sampling_rate = samplingRate;
		int outputType = arff;
		// Get the audio recordings to extract features from and throw an
		// exception
		// if there are none
		RecordingInfo[] recordings = info;
		if (recordings == null)
			throw new Exception(
					"No recordings available to extract features from.");

		// TODO we do not have any updater
		// if (updater != null) {
		// updater.setNumberOfFiles(recordings.length);
		// }

		container = new AggregatorContainer();
		if ((aggregators == null) || (aggregators.length == 0)) {
			aggregators = new Aggregator[3];
			aggregators[0] = new jAudioFeatureExtractor.Aggregators.Mean();
			aggregators[1] = new jAudioFeatureExtractor.Aggregators.StandardDeviation();
			aggregators[2] = new jAudioFeatureExtractor.Aggregators.AreaMoments();
			aggregators[2].setParameters(
					new String[] { "Area Method of Moments of MFCCs" },
					new String[] { "" });
		}
		container.add(aggregators);

		// Prepare to extract features
		FeatureProcessorForMFCC2D processor = new FeatureProcessorForMFCC2D(window_size,
				window_overlap, sampling_rate, normalise, this.features,
				this.defaults, save_features_for_each_window,
				save_overall_recording_features, featureValue, featureKey,
				outputType, cancel_, container);

		// Extract features from recordings one by one and save them in XML
		// files
		// AudioSamples recording_content;
		for (int i = 0; i < recordings.length; i++) {
			//File load_file = new File(recordings[i].file_path);

			// TODO we do not have any updater, so...
			// if (updater != null) {
			// updater.announceUpdate(i, 0);
			// }
			processor.extractFeatures(ins[i], name[i], null);
		}

		// Finalize saved XML files

		processor.finalize();

		// JOptionPane.showMessageDialog(null,
		// "Features successfully extracted and saved.", "DONE",
		// JOptionPane.INFORMATION_MESSAGE);
	}

}
