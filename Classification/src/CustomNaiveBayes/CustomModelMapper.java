package CustomNaiveBayes;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.classifier.naivebayes.training.IndexInstancesMapper.Counter;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

/**
 * Mapper class for the dictionary MixIDText task. This is an identity class which
 * simply outputs the key and the values that it gets, the intermediate key is
 * the document frequency of a word.
 * 
 * @author UP
 * 
 */
/*
 * public class mixMapper extends Mapper<IntWritable, LongWritable,
 * LongWritable, IntWritable> {
 * 
 * @Override protected void map(IntWritable key, LongWritable value, Context
 * context) throws IOException, InterruptedException { context.write(value,
 * key); System.out.println(value); System.out.println(key);
 */

public class CustomModelMapper extends
		Mapper<Text, VectorWritable, IntWritable, VectorWritable> {

	  private static final Pattern SLASH = Pattern.compile("/");

	  private OpenObjectIntHashMap<String> labelIndex;
	  public enum Counter { SKIPPED_INSTANCES }

	  @Override
	  protected void setup(Context ctx) throws IOException, InterruptedException {
	    super.setup(ctx);
	    labelIndex = new OpenObjectIntHashMap<String>();//BayesUtils.readIndexFromCache(ctx.getConfiguration());
	    labelIndex.put(0+"", 0);
	    labelIndex.put(1+"", 1);
	//    labelIndex.put(2+"", 2);
	 //   labelIndex.put(3+"", 3);
	  }
	@Override
	  protected void map(Text labelText, VectorWritable instance, Context ctx) throws IOException, InterruptedException {
	    String label = SLASH.split(labelText.toString())[1];
	   /* if(!label.equals("0")){
	    	System.out.println();
	    }*/
	    if (labelIndex.containsKey(label)) {
	      ctx.write(new IntWritable(labelIndex.get(label)), instance);
	    	
	    } /*else {
	      ctx.getCounter(Counter.SKIPPED_INSTANCES).increment(1);
	    }*/
	  }

}
