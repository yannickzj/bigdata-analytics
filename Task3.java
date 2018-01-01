import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task3 {

  public static class Mapper3
          extends Mapper<Object, Text, IntWritable, IntWritable>{

    private IntWritable user = new IntWritable();
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable zero = new IntWritable(0);
    private static int numCol = 0;

    @Override
    protected void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

      String[] tokens = value.toString().split(",", -1);
      numCol = Math.max(numCol, tokens.length - 1);
      for (int i = 1; i < tokens.length; i++) {
        if (tokens[i].length() > 0) {
          user.set(i);
          context.write(user, one);
        }
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (int i = 0; i < numCol; i++) {
        user.set(i + 1);
        context.write(user, zero);
      }

    }
  }

  public static class Reducer3
          extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values,
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

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }

    Job job = new Job(conf, "Task3");
    job.setJarByClass(Task3.class);
    job.setMapperClass(Task3.Mapper3.class);
    job.setCombinerClass(Task3.Reducer3.class);
    job.setReducerClass(Task3.Reducer3.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
