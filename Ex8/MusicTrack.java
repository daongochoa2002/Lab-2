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

    public static class MusicTrackMapper 
        extends Mapper< Object , Text, IntWritable, Text> {

        private static IntWritable trackId = new IntWritable();
        private static Text data = new Text();

        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

            String[] parts = value.toString().split("[|]");

            trackId.set(Integer.parseInt(parts[0]));
            data.set(parts[1]+" "+parts[2]+" "+parts[3]+" "+parts[4]);
            context.write(trackId, data);
        }
    }
    

    public static class MusicTrackReducer 
        extends Reducer< IntWritable , Text, IntWritable, Text> {

            private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            HashSet<String> uniqueListeners = new HashSet<>();
            int SharedCount =  0;
            int RadioCount =  0;
            int SkipCount =  0;
            for (Text val : values) {
                StringTokenizer itr = new StringTokenizer(val.toString());
                uniqueListeners.add(itr.nextToken());
                SharedCount += Integer.parseInt(itr.nextToken());
                RadioCount += Integer.parseInt(itr.nextToken());
                SkipCount += Integer.parseInt(itr.nextToken());


            }
            result.set(String.valueOf(uniqueListeners.size()) +" "+ String.valueOf(SharedCount)+" "+String.valueOf(RadioCount)+" "+String.valueOf(SkipCount));
            context.write(key,result);
        }
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MusicTrack");
    job.setJarByClass(MusicTrack.class);
    job.setMapperClass(MusicTrackMapper.class);
    job.setReducerClass(MusicTrackReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}