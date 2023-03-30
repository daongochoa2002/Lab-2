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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.Collections;
import java.util.ArrayList;
    import java.util.Arrays;
    import java.util.List;
import java.util.HashSet;

public class FindAllNeighbour {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text k = new Text();
    private Text value = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String[] parts = value.toString().split("[ ]");
        List<Integer> numbers = new ArrayList<Integer>(parts.length) ;
        for(String part: parts){
            numbers.add(Integer.valueOf(part));
        }
        Collections.sort(numbers);
        if(numbers.size() > 0){
            for(int i = 0; i < numbers.size(); i++){
                k.set(String.valueOf(numbers.get(i)));
                for(int j = 0; j < numbers.size(); j++)
                context.write(k,new Text(String.valueOf(numbers.get(j))));
            }
        }
            
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
      String newStr="";
      for(String neighbour: neighbours){
        newStr += neighbour + " ";
      }
      result.set(newStr);
      for(String neighbour: neighbours){
        context.write(new Text(neighbour), result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(FindAllNeighbour.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);
  }
}