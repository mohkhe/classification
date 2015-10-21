package MixIDText;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Reducer class for the dictionary MixIDText class. This is an identity reducer 
 * which simply outputs the values received from the map output.
 * 
 * @author UP
 *
 */
public class mixReducer extends
        Reducer<IntWritable, Type, DoubleWritable, Text> {
 
    @Override
    protected void reduce(IntWritable key, Iterable<Type> value, Context context)
            throws IOException, InterruptedException {
        String longtext = "";
        DoubleWritable freq = null;
    	for(Type val : value) {
    		if(val.textObj.toString().equals("notsetyet!")){
    			freq = val.getDoubleWritableObj();
    		}else{
    			longtext = val.getTextObj().toString();
    		}
    	/*	if(val.longWritableObj != null && (val.longWritableObj.get() != 0)){
    			freq = val.getLongWritableObj();
    		}
    		if ((val.longWritableObj.get() == 0) && val.textObj != null){
    			longtext = val.getTextObj().toString();
    		}*/
    		/*if(val.getClass().toString().equals(LongWritable.class.toString())){
    			freq = (LongWritable) val;
    		}else if (val.getClass().toString().equals(Text.class.toString())){
    			longtext = (String) val;
    		}*/
            // context.write(new Text(val + "," + key), null);
    	//	longtext = val+","+longtext;
        }
    	context.write(freq, new Text(longtext));

    }
 
}
