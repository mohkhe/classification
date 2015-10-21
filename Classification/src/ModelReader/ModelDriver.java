package ModelReader;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * The main driver program for the sorting of the dictionary. This sorts the
 * dictionary by the document frequency of the words.
 * 
 * @author UP
 * 
 */
public class ModelDriver {

	private static final Log logger = LogFactory.getLog(ModelDriver.class);

	/**
	 * This is the main method that drives the creation of the inverted index.
	 * It expects the following input arguments - the location of the input
	 * files the location of the partition files
	 * 
	 * @param args
	 *            - the command line arguments
	 */
	/*mohammad public static void main(String[] args) {
		try {
			runJob(args[0], args[1], args[2], Integer.parseInt(args[3]));
		} catch (Exception ex) {
			logger.error(null, ex);
		}
	}*/

	/**
	 * This creates and runs the job for creating the inverted index
	 * 
	 * @param input
	 *            - location of the input folder
	 * @param output
	 *            - location of the output folder
	 * @param partitionLocation
	 *            - location of the partition folder
	 * @param numReduceTasks
	 *            - number of reduce tasks
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void runJob(String modelPath,
			String sortedFileOutput, int numReduceTasks) throws IOException,
			URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "ModelReaderWriter");
		job.setJarByClass(ModelDriver.class);
		job.setMapperClass(ModelMapper.class);
		job.setReducerClass(ModelReducer.class);
//		job.setPartitionerClass(TotalOrderPartitioner.class);
	//	job.setNumReduceTasks(numReduceTasks);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
	//	job.setInputFormatClass(TextInputFormat.class);
	//	job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
/*		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);*/
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
	//	job.setSortComparatorClass(ModelComparator.class);
	//check if it is necessary	job.setCombinerClass(VectorSumReducer.class);

		
		FileInputFormat.setInputPaths(job, modelPath);
		FileOutputFormat.setOutputPath(job, new Path(sortedFileOutput+".dictionary.sorted" ));//getCurrentDateTime()
		
		/*
		job.setPartitionerClass(TotalOrderPartitioner.class);

		Path inputDir = new Path(partitionLocation);
		Path partitionFile = new Path(inputDir, "partitioning");
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),
				partitionFile);*/

	/*	double pcnt = 10.0;
		int numSamples = numReduceTasks;
		int maxSplits = numReduceTasks - 1;
		if (0 >= maxSplits)
			maxSplits = Integer.MAX_VALUE;

		InputSampler.Sampler sampler = new InputSampler.RandomSampler(pcnt,
				numSamples, maxSplits);
		InputSampler.writePartitionFile(job, sampler);*/

		try {
			job.waitForCompletion(true);
		} catch (InterruptedException ex) {
			logger.error(ex);
		} catch (ClassNotFoundException ex) {
			logger.error(ex);
		}
	}

	/**
	 * Returns todays date and time formatted as "yyyy.MM.dd.HH.mm.ss"
	 * 
	 * @return String - date formatted as yyyy.MM.dd.HH.mm.ss
	 */
	private static String getCurrentDateTime() {
		Date d = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
		return sdf.format(d);
	}

/*	private static void makePartialVectors(Path input, Configuration baseConf,
			int maxNGramSize, Path dictionaryFilePath, Path output,
			int dimension, boolean sequentialAccess, boolean namedVectors,
			int numReducers) throws IOException, InterruptedException,
			ClassNotFoundException {

		Configuration conf = new Configuration(baseConf);
		// this conf parameter needs to be set enable serialisation of conf
		// values
		conf.set(
				"io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,"
						+ "org.apache.hadoop.io.serializer.WritableSerialization");
		conf.setInt(PartialVectorMerger.DIMENSION, dimension);
		conf.setBoolean(PartialVectorMerger.SEQUENTIAL_ACCESS, sequentialAccess);
		conf.setBoolean(PartialVectorMerger.NAMED_VECTOR, namedVectors);
		conf.setInt(MAX_NGRAMS, maxNGramSize);
		DistributedCache.setCacheFiles(
				new URI[] { dictionaryFilePath.toUri() }, conf);

		Job job = new Job(conf);
		job.setJobName("DictionaryVectorizer::MakePartialVectors: input-folder: "
				+ input + ", dictionary-file: " + dictionaryFilePath);
		job.setJarByClass(DictionaryVectorizer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringTuple.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);
		FileInputFormat.setInputPaths(job, input);

		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(Mapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setReducerClass(TFPartialVectorReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(numReducers);

		HadoopUtil.delete(conf, output);

		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
	}
	
	prepareJob(Path inputPath,
                           Path outputPath,
                           Class<? extends InputFormat> inputFormat,
                           Class<? extends Mapper> mapper,
                           Class<? extends Writable> mapperKey,
                           Class<? extends Writable> mapperValue,
                           Class<? extends Reducer> reducer,
                           Class<? extends Writable> reducerKey,
                           Class<? extends Writable> reducerValue,
                           Class<? extends OutputFormat> outputFormat,
                           Configuration conf)
	*/
}
