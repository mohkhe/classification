package ModelReader;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.classifier.naivebayes.training.IndexInstancesMapper;
import org.apache.mahout.common.mapreduce.VectorSumReducer;
import org.apache.mahout.math.VectorWritable;

public class Test {
	private static final Log logger = LogFactory.getLog(ModelDriver.class);

	


public static void runJob(String modelPath,
		String sortedFileOutput, int numReduceTasks) throws IOException,
		URISyntaxException, ClassNotFoundException, InterruptedException {
	Configuration conf = new Configuration();
	
/*	prepareJob(new Path("process/sparce"),
            new Path("process/SUMMED_OBSERVATIONS"),
            SequenceFileInputFormat.class,
            IndexInstancesMapper.class,
            IntWritable.class,
            VectorWritable.class,
            VectorSumReducer.class,
            IntWritable.class,
            VectorWritable.class,
            SequenceFileOutputFormat.class);
indexInstances.setCombinerClass(VectorSumReducer.class);*/

	Job job = new Job(conf, "TestWriter");
	job.setJarByClass(Test.class);
	job.setMapperClass(ModelMapper.class);//IndexInstancesMapper.class);
	job.setReducerClass(VectorSumReducer.class);
//	job.setPartitionerClass(TotalOrderPartitioner.class);
//	job.setNumReduceTasks(numReduceTasks);
	
	job.setInputFormatClass(SequenceFileInputFormat.class);
//	job.setInputFormatClass(TextInputFormat.class);
//	job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(VectorWritable.class);
	job.setCombinerClass(VectorSumReducer.class);
/*		job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(IntWritable.class);*/
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(VectorWritable.class);
//	job.setSortComparatorClass(ModelComparator.class);

	FileInputFormat.setInputPaths(job, modelPath);
	FileOutputFormat.setOutputPath(job, new Path(sortedFileOutput+".dictionary.sorted." + getCurrentDateTime()));
	
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
}