import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SparseRowMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;
import org.apache.mahout.vectorizer.TFIDF;

import sort.SortDriver;
import CustomNaiveBayes.CreateMatriceLableFeatureWeightJob;
import MixIDText.mixDriver;
import ModelReader.ModelDriver;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.io.Closeables;

public class NaiveBayes {

	Configuration configuration = new Configuration();

	String inputFilePath = "input/tweets.txt";
	String inputFileFolder = "input/output/";
	String sequenceFilePath = "process/seq";
	String labelIndexPath = "process/label/labelindex";
	String modelPath = "process/model";
	String vectorsPath = "process/sparce";
	String dictionaryPath = "process/sparce/dictionary.file-0";
	// String documentFrequencyPath = "process/sparce/df-count/part-r-00000";
	String documentFrequencyPath = "process/sorted/output/output.dictionary.sorted/part-r-00000";

	String mixDriverOutputFile = "process/mixed/output/";
	String dictionaryFreqFile = "process/mixed/output/part-r-00000";
	String sortedFile = "process/sorted/output";

	FileSystem fs;
	Path seqFilePath;
	SequenceFile.Writer writer;
	int totalEmptyCat0 = 0;
	int totalEmptyCat1 = 0;

	public static void main(String[] args) throws Throwable {
		NaiveBayes nb = new NaiveBayes();
		// nb.inputDataToSequenceFileCategoryTextOneLine();
		nb.inputDataToSequenceFileCategoryTextOneFile();
		 nb.sequenceFileToSparseVector();
		 nb.trainNaiveBayesModel();
		 nb.CreateMatriceLableFeatureWeights();
		 nb.getTop1000();
		nb.mixFreqDictionary();
		nb.sort();
		// nb.classifyNewTweet("Have a nice day!");
	}

	// copied the file here from process/sorter/output/dis+date/part-r-0000
	private void getTop1000() {
		try {
			ModelDriver.runJob("process/sorted/output/part-r-00000", sortedFile
					+ "/output", 1);

		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void CreateMatriceLableFeatureWeights() {
		/*
		 * String SUMMED_OBSERVATIONS = "summedObservations"; String WEIGHTS =
		 * "weights"; String THETAS = "thetas"; // new Path(tempPath, directory)
		 * try { HadoopUtil.prepareJob(new Path("temp/"+SUMMED_OBSERVATIONS),
		 * new Path("temp/"+THETAS), SequenceFileInputFormat.class,
		 * ThetaMapper.class, Text.class, VectorWritable.class,
		 * VectorSumReducer.class, Text.class, VectorWritable.class,
		 * SequenceFileOutputFormat.class, configuration); } catch (IOException
		 * e1) { // TODO Auto-generated catch block e1.printStackTrace(); }
		 */
		/*
		 * Matrix scoresPerLabelAndFeature = new SparseMatrix(2, 52); for
		 * (Pair<IntWritable,VectorWritable> entry : new
		 * SequenceFileDirIterable<IntWritable,VectorWritable>( new
		 * Path("process/part-r-00000"), PathType.LIST,
		 * PathFilters.partFilter(), new Configuration())) { // Matrix
		 * scoresPerLabelAndFeature = null;
		 * scoresPerLabelAndFeature.assignRow(entry.getFirst().get(),
		 * entry.getSecond().get()); }
		 */

		try {
			CreateMatriceLableFeatureWeightJob.runJob(
					"process/sparce/tfidf-vectors", sortedFile, 1);
			/*
			 * ModelDriver .runJob("process/part-r-00000", sortedFile +
			 * "/model", 1);
			 */
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void sort() {

		// SortDriver sortDriver = new SortDriver();
		try {
			SortDriver.runJob(dictionaryFreqFile, sortedFile + "/sortedoutput",
					1);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void mixFreqDictionary() {
		// mixDriver sortDriver = new mixDriver();
		try {
			mixDriver.runJob(documentFrequencyPath, dictionaryPath,
					mixDriverOutputFile, 1);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Model is a matrix (wordId, labelId) => probability score

		/*
		 * try { model = materialize(new Path(modelPath), configuration); }
		 * catch (IOException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }// NaiveBayesModel.materialize(new //
		 * Path(modelPath), // configuration); StandardNaiveBayesClassifier
		 * classifier = new StandardNaiveBayesClassifier( model);
		 * 
		 * // With the classifier, we get one score for each label.The label
		 * with // the highest score is the one the tweet is more likely to be
		 * // associated to Vector resultVector =
		 * classifier.classifyFull(vector);
		 * 
		 * double bestScore = -Double.MAX_VALUE; int bestCategoryId = -1; for
		 * (int i = 0; i < resultVector.size(); i++) { Element element =
		 * resultVector.getElement(i); int categoryId = element.index(); double
		 * score = element.get(); if (score > bestScore) { bestScore = score;
		 * bestCategoryId = categoryId; } if (categoryId == 1) {
		 * System.out.println("Probability of being positive: " + score); } else
		 * { System.out.println("Probability of being negative: " + score); } }
		 */
		/*
		 * for (Element element : resultVector)) { int categoryId =
		 * element.index(); double score = element.get(); if (score > bestScore)
		 * { bestScore = score; bestCategoryId = categoryId; } if (categoryId ==
		 * 1) { System.out.println("Probability of being positive: " + score); }
		 * else { System.ou; Path seqFilePath; SequenceFile.Writer
		 * writer;t.println("Probability of being negative: " + score); } }
		 */
		/*
		 * Text * if (bestCategoryId == 1) {
		 * System.out.println("The tweet is positive :) "); } else {
		 * System.out.println("The tweet is negative :( "); } analyzer.close();
		 */

	}

	public void inputDataToSequenceFileCategoryTextOneFile() throws Exception {
		File folder = new File(inputFileFolder);
		seqFilePath = new Path(sequenceFilePath);
		int count = 0;
		fs = FileSystem.getLocal(configuration);
		fs.delete(seqFilePath, false);
		writer = SequenceFile.createWriter(fs, configuration, seqFilePath,
				Text.class, Text.class);

		int category = 0;
		if (folder.isDirectory()) {
			File[] folderS = folder.listFiles();
			for (File file : folderS) {
				if (file.isDirectory()) {
					String name = file.getName();
					if (name.equals("changed")) {
						category = 0;
					} else if (name.equals("newdocs")) {
						category = 0;//2
					} else if (name.equals("removeddocs")) {
						category = 0;//3
					} else if (name.equals("unchanged")) {
						category = 1;
					}
					/*
					 * switch (Integer.valueOf(name)){ case 0: category = 0;
					 * case Integer.valueOf(name): category = 0; case
					 * Integer.valueOf(name): category = 0; case
					 * Integer.valueOf(name): category = 0; }
					 */
					File[] files = file.listFiles();
					for (File f : files) {
						read(f.getAbsolutePath(), category, count);
						count++;
					}
				}
			}
		}
		writer.close();
		System.out.println("totalEmptyCat0 = " + this.totalEmptyCat0);
		System.out.println("totalEmptyCat1 = " +this.totalEmptyCat1);

	}

	private void read(String filePath, int category, int count) {
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(filePath));

			// int count = 0;
			try {
				String line;
				String text = "";
				while ((line = reader.readLine()) != null) {
					// String[] tokens = line.split("\t");
					text = line + " " + text;

				}
				if (text.equals("")) {
					if (category == 0) {
						totalEmptyCat0++;
					} else if (category == 1) {
						{
							totalEmptyCat1++;
						}
					}
				}
				writer.append(new Text("/" + category + "/doc" + count),
						new Text(text));
			} finally {
				reader.close();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void inputDataToSequenceFileCategoryTextOneLine() throws Exception {
		BufferedReader reader = new BufferedReader(
				new FileReader(inputFilePath));
		FileSystem fs = FileSystem.getLocal(configuration);
		Path seqFilePath = new Path(sequenceFilePath);
		fs.delete(seqFilePath, false);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs,
				configuration, seqFilePath, Text.class, Text.class);
		int count = 0;
		try {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] tokens = line.split("\t");
				writer.append(new Text("/" + tokens[0] + "/tweet" + count++),
						new Text(tokens[1]));
			}
		} finally {
			reader.close();
			writer.close();
		}
	}

	void sequenceFileToSparseVector() throws Exception {
		SparseVectorsFromSequenceFiles svfsf = new SparseVectorsFromSequenceFiles();
		svfsf.run(new String[] { "-i", sequenceFilePath, "-o", vectorsPath,
				"-ow" });
	}

	void trainNaiveBayesModel() throws Exception {
		TrainNaiveBayesJob trainNaiveBayes = new TrainNaiveBayesJob();
		trainNaiveBayes.setConf(configuration);
		trainNaiveBayes.run(new String[] { "-i",
				vectorsPath + "/tfidf-vectors", "-o", modelPath, "-li",
				labelIndexPath, "-el", "-c", "-ow" });//
	}

	private void classifyNewTweet(String tweet) throws IOException {
		System.out.println("Tweet: " + tweet);

		Map<String, Integer> dictionary = readDictionary(configuration,
				new Path(dictionaryPath));
		Map<Integer, Long> documentFrequency = readDocumentFrequency(
				configuration, new Path(documentFrequencyPath));

		Multiset<String> words = ConcurrentHashMultiset.create();

		// Extract the words from the new tweet using Lucene
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);// Version.LUCENE_46);
		TokenStream tokenStream = analyzer.tokenStream("text",
				new StringReader(tweet));
		CharTermAttribute termAttribute = tokenStream
				.addAttribute(CharTermAttribute.class);
		tokenStream.reset();
		int wordCount = 0;
		while (tokenStream.incrementToken()) {
			if (termAttribute.length() > 0) {
				String word = tokenStream.getAttribute(CharTermAttribute.class)
						.toString();
				Integer wordId = dictionary.get(word);
				// If the word is not in the dictionary, skip it
				if (wordId != null) {
					words.add(word);
					wordCount++;
				}
			}
		}
		tokenStream.end();
		tokenStream.close();

		int documentCount = documentFrequency.get(-1).intValue();

		// Create a vector for the new tweet (wordId => TFIDF weight)
		Vector vector = new RandomAccessSparseVector(10000);
		TFIDF tfidf = new TFIDF();
		for (Multiset.Entry<String> entry : words.entrySet()) {
			String word = entry.getElement();
			int count = entry.getCount();
			Integer wordId = dictionary.get(word);
			Long freq = documentFrequency.get(wordId);
			double tfIdfValue = tfidf.calculate(count, freq.intValue(),
					wordCount, documentCount);
			vector.setQuick(wordId, tfIdfValue);
		}

		// Model is a matrix (wordId, labelId) => probability score
		NaiveBayesModel model = materialize(new Path(modelPath), configuration);// NaiveBayesModel.materialize(new
																				// Path(modelPath),
																				// configuration);
		StandardNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(
				model);

		// With the classifier, we get one score for each label.The label with
		// the highest score is the one the tweet is more likely to be
		// associated to
		Vector resultVector = classifier.classifyFull(vector);

		double bestScore = -Double.MAX_VALUE;
		int bestCategoryId = -1;
		for (int i = 0; i < resultVector.size(); i++) {
			Element element = resultVector.getElement(i);
			int categoryId = element.index();
			double score = element.get();
			if (score > bestScore) {
				bestScore = score;
				bestCategoryId = categoryId;
			}
			if (categoryId == 1) {
				System.out.println("Probability of being positive: " + score);
			} else {
				System.out.println("Probability of being negative: " + score);
			}
		}
		/*
		 * for (Element element : resultVector)) { int categoryId =
		 * element.index(); double score = element.get(); if (score > bestScore)
		 * { bestScore = score; bestCategoryId = categoryId; } if (categoryId ==
		 * 1) { System.out.println("Probability of being positive: " + score); }
		 * else { System.out.println("Probability of being negative: " + score);
		 * } }
		 */
		if (bestCategoryId == 1) {
			System.out.println("The tweet is positive :) ");
		} else {
			System.out.println("The tweet is negative :( ");
		}
		analyzer.close();
	}

	public static Map<String, Integer> readDictionary(Configuration conf,
			Path dictionnaryPath) {
		Map<String, Integer> dictionnary = new HashMap<String, Integer>();
		for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(
				dictionnaryPath, true, conf)) {
			dictionnary.put(pair.getFirst().toString(), pair.getSecond().get());
		}
		return dictionnary;
	}

	public static Map<Integer, Long> readDocumentFrequency(Configuration conf,
			Path documentFrequencyPath) {
		Map<Integer, Long> documentFrequency = new HashMap<Integer, Long>();
		for (Pair<IntWritable, LongWritable> pair : new SequenceFileIterable<IntWritable, LongWritable>(
				documentFrequencyPath, true, conf)) {
			documentFrequency
					.put(pair.getFirst().get(), pair.getSecond().get());
		}
		return documentFrequency;
	}

	public static NaiveBayesModel materialize(Path output, Configuration conf)
			throws IOException {
		FileSystem fs = output.getFileSystem(conf);

		Vector weightsPerLabel = null;
		Vector perLabelThetaNormalizer = null;
		Vector weightsPerFeature = null;
		Matrix weightsPerLabelAndFeature;
		float alphaI;

		FSDataInputStream in = fs.open(new Path(output, "naiveBayesModel.bin"));
		try {
			alphaI = in.readFloat();
			weightsPerFeature = VectorWritable.readVector(in);
			double maxValue = weightsPerFeature.maxValue();
			// org.apache.mahout.math.Sorting.
			// Collections.sort(weightsPerFeature.maxValue());
			weightsPerLabel = new DenseVector(VectorWritable.readVector(in));
			// MixIDText(weightsPerFeature);
			perLabelThetaNormalizer = new DenseVector(
					VectorWritable.readVector(in));

			weightsPerLabelAndFeature = new SparseRowMatrix(
					weightsPerLabel.size(), weightsPerFeature.size());
			for (int label = 0; label < weightsPerLabelAndFeature.numRows(); label++) {
				weightsPerLabelAndFeature.assignRow(label,
						VectorWritable.readVector(in));
			}

		} finally {
			Closeables.close(in, true);
		}
		NaiveBayesModel model = new NaiveBayesModel(weightsPerLabelAndFeature,
				weightsPerFeature, weightsPerLabel, perLabelThetaNormalizer,
				alphaI);
		model.validate();
		return model;
	}

	/*
	 * public static org.apache.mahout.math.VectorView MixIDText(final Vector
	 * vector) { int[] indexes = new int[vector.size()]; // row indexes to
	 * reorder instead of matrix itself for (int i = indexes.length; --i >= 0;)
	 * { indexes[i] = i; }
	 * 
	 * IntComparator comp = new IntComparator() { public int compare(int a, int
	 * b) { double av = vector.getQuick(a); double bv = vector.getQuick(b); if
	 * (av != av || bv != bv) { return compare(av, bv); } // swap NaNs to the
	 * end return av < bv ? -1 : (av == bv ? 0 : 1); }
	 * 
	 * private int compare(double av, double bv) { // TODO Auto-generated method
	 * stub return 0; } };
	 * 
	 * runSort(indexes, 0, indexes.length, comp);
	 * 
	 * return vector; }
	 * 
	 * private static void runSort(int[] indexes, int i, int length,
	 * IntComparator comp) { // TODO Auto-generated method stub
	 * 
	 * }
	 */
}