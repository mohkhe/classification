package sort;

import org.apache.hadoop.io.LongWritable;
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
public class SortComparator extends WritableComparator {
     
    protected SortComparator() {
        super(LongWritable.class, true);
    }
 
    /**
     * Compares in the descending order of the keys.
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
    	LongWritable o1 = (LongWritable) a;
    	LongWritable o2 = (LongWritable) b;
        if(o1.get() < o2.get()) {
            return 1;
        }else if(o1.get() > o2.get()) {
            return -1;
        }else {
            return 0;
        }
    }
     
}
