package org.example;

// Taken from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Source_Code

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount {
  public static class TokenizerMapper
          extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
          extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static class KeyValueSwapper
          extends Mapper<Text, Text, IntWritable, Text> {

    IntWritable frequency = new IntWritable();

    public void map(Text key, Text value, Context context
    ) throws IOException, InterruptedException {

      int newVal = Integer.parseInt(value.toString());
      frequency.set(newVal);
      context.write(frequency, key);
    }
  }

  public static class WordReducer
          extends Reducer<IntWritable, Text, IntWritable, Text> {
    private Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
      String sum = "";
      for (Text val : values) {
        String v = val.toString();
        if (v.trim().equals("")){
          continue;
        }
        if (sum.equals("")) {
          sum = "" + v;
        } else {
          sum = sum + ", " + v;
        }
      }
      result.set(sum);
      context.write(key, result);
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    // first job
    Job job1 = Job.getInstance(conf, "word count");
    job1.setJarByClass(WordCount.class);

    job1.setMapperClass(TokenizerMapper.class);
    job1.setCombinerClass(IntSumReducer.class);
    job1.setReducerClass(IntSumReducer.class);

    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job1, new Path(args[0]));
    Path out = new Path(args[1]);
    FileOutputFormat.setOutputPath(job1, new Path(out, "out1"));
    job1.waitForCompletion(true);
    
    // second job
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "mapreduce2");
    job2.setJarByClass(WordCount.class);

    job2.setMapperClass(KeyValueSwapper.class);
    job2.setCombinerClass(WordReducer.class);
    job2.setReducerClass(WordReducer.class);

    job2.setInputFormatClass(KeyValueTextInputFormat.class);
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job2, new Path(out, "out1"));
    FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
