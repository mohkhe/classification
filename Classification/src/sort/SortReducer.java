package sort;

import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Reducer class for the dictionary MixIDText class. This is an identity reducer 
 * which simply outputs the values received from the map output.
 * 
 * @author UP
 *
 */
public class SortReducer extends
        Reducer<DoubleWritable, Text, Text, DoubleWritable> {
	HashSet<String> listOfStopWords;

    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException {
        String longtext = "";
   //     LongWritable freq = null;
    	for(Text val : value) {
    		/*if(val.textObj.toString().equals("notsetyet!")){
    			freq = val.getLongWritableObj();
    		}else{
    			longtext = val.getTextObj().toString();
    		}*/
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
    		
    		longtext = val.toString();
    		
    		if (listOfStopWords ==null){
    			this.setListOfStopWords();
    		}
    		if (checkFormat(val)) {
        		context.write(new Text(longtext), key);

    		}
    		//longtext = val+","+longtext;
        }
    	

    }
	private boolean checkFormat(Text value) {
		boolean keep = false;
		String strVal = value.toString();
		if (listOfStopWords.contains(strVal)) {
			return false;
		} else if (strVal.contains("\n") || strVal.contains("\\")
				|| strVal.contains(":") || strVal.contains(";")
				|| strVal.contains(".")) {
			return false;
		} else if (strVal.length() < 2 || strVal.equals("")) {
			return false;
		}/* else if (strVal.length() < 3) {
			return false;
		}*/
		if (strVal.matches(".*\\d.*")) {
			return false;
		}
		if (strVal.contains(".*\\d.*")) {
			return false;
		}

		if (listOfStopWords.contains(strVal)) {
			return false;
		}

		if (strVal.contains(",")
				// || query.contains(".")
				|| strVal.contains("'") || strVal.contains("_")
				|| strVal.contains("+") || strVal.contains("=")
				|| strVal.contains(")") || strVal.contains("(")
				|| strVal.contains("*") || strVal.contains("&")
				|| strVal.contains("^") || strVal.contains("%")
				|| strVal.contains("$") || strVal.contains("#")
				|| strVal.contains("@") || strVal.contains("!")
				|| strVal.contains("~") || strVal.contains("`")
				|| strVal.contains("?") || strVal.contains("\\")
				|| strVal.contains("/") || strVal.contains("\\.")
				|| strVal.contains("/") || strVal.contains("/")
				|| strVal.contains("/") || strVal.contains("/")
				|| strVal.matches("[_@#$%&!~`+\\-*/\\^ \\.,?!:;=()\\']+")) {
			return false;
		}
		if (strVal.contains("(.)*(.)\\1{2,}(.)*")) {
			return false;
		}
	/*	Pattern pattern = Pattern.compile("(.)*\\1{2,}(.)*",
				Pattern.CASE_INSENSITIVE);
		Matcher m = pattern.matcher(strVal);
		if (m.matches()) {
			return false;
		}*/
		if (strVal.matches("(.)\\1{2,}")) {
			return false;
		}
		return true;
	}

	// public String[] tokenizer(String content) {
	// // String delims = "[+\\-*/\\^ .,?!:;=()]+";
	// String[] tokens = content.split(delims);
	// return tokens;
	// }

	HashSet<String> setListOfStopWords() {
		listOfStopWords = new HashSet<String>();
		listOfStopWords.add("the");
		listOfStopWords.add("and");
		listOfStopWords.add("not");
		listOfStopWords.add("from");
		listOfStopWords.add("for");
		listOfStopWords.add("you");
		listOfStopWords.add("your");
		listOfStopWords.add("one");
		listOfStopWords.add("other");
		listOfStopWords.add("others");
		listOfStopWords.add("over");
		listOfStopWords.add("home");
		listOfStopWords.add("our");
		listOfStopWords.add("first");
		listOfStopWords.add("yes");
		listOfStopWords.add("re");
		listOfStopWords.add("vs");
		listOfStopWords.add("been");
		
		listOfStopWords.add("a");
		listOfStopWords.add("an");
		listOfStopWords.add("of");
		listOfStopWords.add("in");
		listOfStopWords.add("is");
		listOfStopWords.add("to");
		listOfStopWords.add("at");
		listOfStopWords.add("on");
		listOfStopWords.add("as");
		listOfStopWords.add("by");
		listOfStopWords.add("up");
		listOfStopWords.add("us");
		listOfStopWords.add("my");
		listOfStopWords.add("do");
		listOfStopWords.add("he");
		listOfStopWords.add("so");
		listOfStopWords.add("to");
		listOfStopWords.add("too");
		
		listOfStopWords.add("het");
		listOfStopWords.add("de");
		listOfStopWords.add("en");
		listOfStopWords.add("met");
		listOfStopWords.add("andere");
		listOfStopWords.add("tussen");
		listOfStopWords.add("van");
		listOfStopWords.add("een");
		listOfStopWords.add("je");	
		listOfStopWords.add("te");
		listOfStopWords.add("om");
		listOfStopWords.add("op");
		
		// listOfStopWords.add("pagina");
		listOfStopWords.add("deze");

		listOfStopWords.add("can");
		listOfStopWords.add("could");
		listOfStopWords.add("have");
		listOfStopWords.add("had");
		listOfStopWords.add("will");
		listOfStopWords.add("would");
		listOfStopWords.add("there");
		listOfStopWords.add("with");
		// listOfStopWords.add("wikipedia");
		// listOfStopWords.add("wikimedia");
		listOfStopWords.add("also");
		// listOfStopWords.add("org");
		listOfStopWords.add("here");
		listOfStopWords.add("there");
		// listOfStopWords.add("data");
		listOfStopWords.add("that");
		listOfStopWords.add("this");
		listOfStopWords.add("these");
		listOfStopWords.add("those");
		listOfStopWords.add("me");
		listOfStopWords.add("her");
		listOfStopWords.add("his");
		// listOfStopWords.add("world");
		listOfStopWords.add("at");
		listOfStopWords.add("was");
		listOfStopWords.add("were");
		listOfStopWords.add("we");
		// listOfStopWords.add("page");
		// listOfStopWords.add("new");
		listOfStopWords.add("all");
		listOfStopWords.add("also");
		// listOfStopWords.add("public");
		// listOfStopWords.add("next");
		// listOfStopWords.add("last");
		// listOfStopWords.add("book");
		listOfStopWords.add("than");
		listOfStopWords.add("which");
		listOfStopWords.add("when");

		// listOfStopWords.add("see");
		// listOfStopWords.add("many");
		listOfStopWords.add("has");
		listOfStopWords.add("are");
		listOfStopWords.add("com");
		listOfStopWords.add("or");
		listOfStopWords.add("more");
		listOfStopWords.add("be");
		listOfStopWords.add("its");
		// listOfStopWords.add("data");
		// listOfStopWords.add("please");
		// listOfStopWords.add("http");
		// listOfStopWords.add("links");
		// listOfStopWords.add("their");
		listOfStopWords.add("page");
		listOfStopWords.add("about");
		// listOfStopWords.add("high");
		listOfStopWords.add("must");
		listOfStopWords.add("see");
		// listOfStopWords.add("book");
		return listOfStopWords;
	}
}
