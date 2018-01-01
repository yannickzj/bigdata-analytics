import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task2 {

  public static class Mapper2
          extends Mapper<Object, Text, Text, IntWritable>{

    private static final Text movie = new Text("*");
    private IntWritable num = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

      String[] tokens = value.toString().split(",");
      int total = 0;
      for (int i = 1; i < tokens.length; i++) {
        if (tokens[i].length() > 0) {
          total++;
        }
      }

      num.set(total);
      context.write(movie, num);
    }
  }

  public static class Combiner2
          extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
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

  public static class Reducer2
          extends Reducer<Text, IntWritable, LongWritable, NullWritable> {
    private LongWritable result = new LongWritable();
    private static final NullWritable empty = NullWritable.get();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
      long sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(result, empty);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", "");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }

    Job job = new Job(conf, "Task2");
    job.setJarByClass(Task2.class);
    job.setMapperClass(Task2.Mapper2.class);
    job.setCombinerClass(Task2.Combiner2.class);
    job.setReducerClass(Task2.Reducer2.class);
    job.setNumReduceTasks(1);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(NullWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
