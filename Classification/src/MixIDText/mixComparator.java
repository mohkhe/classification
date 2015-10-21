package MixIDText;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
 
/**
 * This comparator is used to MixIDText the output of the dictionary in the 
 * descending order of the counts. The descending order enables to pick a 
 * dictionary of the required size by any aplication using the dictionary.
 * 
 * @author UP
 *
 */
public class mixComparator extends WritableComparator {
     
    protected mixComparator() {
        super(IntWritable.class, true);
    }
 
    /**
     * Compares in the descending order of the keys.
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
    	
    	IntWritable o1 = (IntWritable) a;
    	IntWritable o2 = (IntWritable) b;
        if(o1.get() < o2.get()) {
            return 1;
        }else if(o1.get() > o2.get()) {
            return -1;
        }else {
            return 0;
        }
    }
     
}
