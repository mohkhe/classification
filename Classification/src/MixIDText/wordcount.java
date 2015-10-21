package MixIDText;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class wordcount
{
    public static class Map extends Mapper
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(BytesWritable key, BytesWritable value, Context context) throws IOException, InterruptedException
        {
            System.out.println(key.toString());
            System.out.println(value.toString());
            context.write(key, value);
        }
    }
    public static class Reduce extends Reducer
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    public static void main(String[] args) throws Exception
    {
        FileUtil.fullyDelete(new File("/media/mohammad/DATA/dropbox/Dropbox/ubuntu workspace/Classification/process/sparce/wordcount"));

        Configuration conf = new Configuration();

        Job job = new Job(conf, "wordcount");

        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/media/mohammad/DATA/dropbox/Dropbox/ubuntu workspace/Classification/process/sparce/frequency.file-0"));
        FileOutputFormat.setOutputPath(job, new Path("/media/mohammad/DATA/dropbox/Dropbox/ubuntu workspace/Classification/process/sparce/wordcount"));

        job.setJarByClass(wordcount.class);

        job.waitForCompletion(true);
    }
}