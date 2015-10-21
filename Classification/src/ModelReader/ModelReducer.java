package ModelReader;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class for the dictionary MixIDText class. This is an identity reducer
 * which simply outputs the values received from the map output.
 * 
 * @author UP
 *
 */
public class ModelReducer
		extends
		Reducer<IntWritable, Text, IntWritable, DoubleWritable> {

	@Override
	protected void reduce(IntWritable key,
			Iterable<Text> values, Context ctx) throws IOException,
			InterruptedException {
		double sum = 0;
		double valueCat0 = 0;
		double valueCat1 = 0;
		for(Text value : values){
			String[] splits = value.toString().split("\t");
			String category = splits[0];
			if(category.equals("0")){
				valueCat0 = Double.parseDouble(splits[1]);
			}else if(category.equals("1")){
				valueCat1 = Double.parseDouble(splits[1]);
			}
		}
/*		if(valueCat0!=0 && valueCat1!=0){
		//	valueCat0 = 0;
		//	valueCat1 = 1;//to have sum =0
			System.out.println("hi");
		}*/
		if(valueCat0==0 && valueCat1==0){
			valueCat0 = 0;
			valueCat1 = 1;//to have sum =0
		}else if(valueCat0==0){
			valueCat0 = 1;//to make difference btw 0/1, 0/2
		}else if(valueCat1==0){
			valueCat1 = 1;
		}
		sum = valueCat0/valueCat1;
	/*	if(sum==0){
			sum =0;
		}*/
		ctx.write(key, new DoubleWritable(sum));
		
/*		String[] splits = value.toString().split("\t");
		String category = splits[0];
		String val = splits[1];
		doubl = Double.parseDouble(val);
		if(doubl>biggerValue){
			biggerValue = doubl;
			biggerCategory = category;
		}
		//new DoubleWritable(val);
		sum = Math.abs((doubl-sum));*/
		//ctx.write(key, new VectorWritable(Vectors.sum(values.iterator())));
	}

}
