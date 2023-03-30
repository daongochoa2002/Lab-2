import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.Collections;
import java.util.ArrayList;
    import java.util.Arrays;
    import java.util.List;
import java.util.HashSet;

public class MinimumNeighbour {

    public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text k = new Text();
    private Text value = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        context.write(new Text(parts[0]), new Text(parts[1]));
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      HashSet<String> neighbours = new HashSet<>();
      for (Text val : values) {
        StringTokenizer itr = new StringTokenizer(val.toString());
        while (itr.hasMoreTokens()) {
          neighbours.add(itr.nextToken());
        }
        
      }
      List<String> list = new ArrayList<String>(neighbours);
      Collections.sort(list);
      context.write(new Text(list.get(0)),new Text(" "));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MinimumNeighbour");
    job.setJarByClass(MinimumNeighbour.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }
}