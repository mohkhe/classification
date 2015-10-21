package MixIDText;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class for the dictionary MixIDText task. This is an identity class which
 * simply outputs the key and the values that it gets, the intermediate key is
 * the document frequency of a word.
 * 
 * @author UP
 * 
 */
public class mixMapper2 extends
		Mapper<Text, IntWritable, IntWritable, Type> {

	@Override
	protected void map(Text key, IntWritable value, Context context)
			throws IOException, InterruptedException {
		Type typeObj = new Type(key);
		typeObj.doubleWritableObj = new DoubleWritable(0);
		
		context.write(value, typeObj);
	//	System.out.println(value);
	//	System.out.println(key);

		/*
		 * String val = value.toString(); if (val != null && !val.isEmpty() &&
		 * val.length() >= 5) { String[] splits = val.split("\t");
		 * if(splits.length==3){ context.write(new
		 * IntWritable(Integer.parseInt(splits[1])), new Text(splits[0] + "," +
		 * splits[2])); }else if (splits.length==2){ context.write(new
		 * IntWritable(Integer.parseInt(splits[1])), new Text(splits[0] + "," +
		 * splits[2])); } if (val.contains("\t")){ splits =
		 * value.toString().split("\t"); }
		 */

		// }
	}

}
