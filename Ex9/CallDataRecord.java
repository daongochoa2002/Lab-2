import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;

public class CallDataRecord {

  public static class TokenizerMapper extends Mapper< Object , Text, Text, LongWritable> {
    Text phoneNumber = new Text();
    LongWritable durationInMinutes = new LongWritable();
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
            String[] parts = value.toString().split("[|]");
            if (parts[4].equalsIgnoreCase("1")) {
                phoneNumber.set(parts[0]);
                String callEndTime = parts[3];
                String callStartTime = parts[2];
                long duration = toMillis(callEndTime) - toMillis(callStartTime);
                durationInMinutes.set(duration / (1000 * 60));
                context.write(phoneNumber, durationInMinutes);
            }
        }

        private long toMillis(String date) {
            SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss");
            Date dateFrm = null;
            try {
                dateFrm = format.parse(date);
                } catch (ParseException e) {
                e.printStackTrace();
            }
            return dateFrm.getTime();
        }
    }

  public static class SumReducer extends Reducer< Text , LongWritable, Text, LongWritable> {
    private LongWritable result = new LongWritable();
    public void reduce(Text key, Iterable< LongWritable> values, Context context)
        throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            this.result.set(sum);
            if (sum >= 60) {
                context.write(key, this.result);
            }
        }
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "CallDataRecord");
    job.setJarByClass(CallDataRecord.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(SumReducer.class);
    job.setReducerClass(SumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}