package ModelReader;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

/**
 * Mapper class for the dictionary MixIDText task. This is an identity class
 * which simply outputs the key and the values that it gets, the intermediate
 * key is the document frequency of a word.
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

public class ModelMapper extends
		Mapper<IntWritable, VectorWritable, IntWritable, Text> {// DoubleWritable

	private static final Pattern SLASH = Pattern.compile("/");

	private OpenObjectIntHashMap<String> labelIndex;

	public enum Counter {
		SKIPPED_INSTANCES
	}

	String labelKeeper;

	/*
	 * @Override protected void setup(Context ctx) throws IOException,
	 * InterruptedException { super.setup(ctx); labelIndex = new
	 * OpenObjectIntHashMap
	 * <String>();//BayesUtils.readIndexFromCache(ctx.getConfiguration());
	 * labelIndex.put(0+"", 0); labelIndex.put(1+"", 1); }
	 */
	@Override
	protected void map(IntWritable label, VectorWritable instance, Context ctx)
			throws IOException, InterruptedException {
		// String label = SLASH.split(labelText.toString())[1];
		// if (labelIndex.containsKey(label)) {
		for (Element elm : instance.get().all()) {
			// String className = elm.getClass().toString();
			double value = elm.get();
			int termID = elm.index();
			/*
			 * String[] splits = elm.toString().split(":"); String termIDName =
			 * splits[0]; String termWeightForLabel = splits[1];
			 */
			// System.out.println(termID);
			/*if (label.get() == 0) {
				//String termIDName = "";
				labelKeeper = label.toString();
			}
			if (label.get() != 0) {
				labelKeeper = label.toString();
			}*/
			ctx.write(new IntWritable(termID), new Text(label + "\t"
					+ new DoubleWritable(value)));
			//System.out.print
		}

		/*
		 * } else { ctx.getCounter(Counter.SKIPPED_INSTANCES).increment(1); }
		 */
	}

}
