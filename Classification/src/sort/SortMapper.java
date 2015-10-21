package sort;

import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
public class SortMapper extends
		Mapper<DoubleWritable, Text, DoubleWritable, Text> {
	HashSet<String> listOfStopWords;
	

	@Override
	protected void map(DoubleWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// Type typeObj = new Type(value);
		// typeObj.textObj = new Text("notsetyet!");

	//	if (listOfStopWords ==null){
	//		this.setListOfStopWords();
	//	}
	//	if (checkFormat(value)) {
			context.write(key, value);

	//	}
		// System.out.println(value);
		// System.out.println(key);
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

	private boolean checkFormat(Text value) {
		boolean keep = false;
		String strVal = value.toString();
		if (listOfStopWords.contains(strVal)) {
			return false;
		} else if (strVal.contains("\n") || strVal.contains("\\")
				|| strVal.contains(":") || strVal.contains(";")
				|| strVal.contains(".")) {
			return false;
		} else if (strVal.length() < 2 || strVal.contains("")) {
			return false;
		} /*else if (strVal.length() < 3) {
			return true;
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
		Pattern pattern = Pattern.compile("(.)*\\1{2,}(.)*",
				Pattern.CASE_INSENSITIVE);
		Matcher m = pattern.matcher(strVal);
		if (m.matches()) {
			return false;
		}
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

		listOfStopWords.add("het");
		listOfStopWords.add("de");
		listOfStopWords.add("en");
		listOfStopWords.add("met");
		listOfStopWords.add("andere");
		listOfStopWords.add("tussen");
		listOfStopWords.add("van");
		listOfStopWords.add("een");
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
