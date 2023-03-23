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
import java.util.HashSet;

public class MusicTrack {

    public static class UniqueListenersMapper 
        extends Mapper< Object , Text, IntWritable, IntWritable > {

        private static IntWritable trackId = new IntWritable();
        private static IntWritable userId = new IntWritable();

        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString(),"|");

            while (itr.hasMoreTokens()) {
                userId.set(Integer.parseInt(itr.nextToken().trim()));
                trackId.set(Integer.parseInt(itr.nextToken().trim()));
                context.write(trackId, userId);
                break;
            }
        }
    }
    

    public static class UniqueListenersReducer 
        extends Reducer< IntWritable , IntWritable, IntWritable, IntWritable> {

            private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

            HashSet<IntWritable> set = new HashSet<>();
            for (IntWritable val : values) {
                set.add(val);
            }
            result.set(set.size());
            context.write(key,result);
        }
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "music track");
    job.setJarByClass(MusicTrack.class);
    job.setMapperClass(UniqueListenersMapper.class);
    job.setReducerClass(UniqueListenersReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}