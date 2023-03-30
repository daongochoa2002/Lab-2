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

public class WeatherData {
    public static class MaxTemperatureMapper 
            extends Mapper<Object, Text, Text, Text> {
            //private static Text date = new Text();

        public void map(Object key, Text Value,Context context)
        throws IOException, InterruptedException {

            String line = Value.toString();
            // Example of Input
            // Date Max Min
            // 25380 20130101 2.514 -135.69 58.43 8.3 1.1 4.7 4.9 5.6 0.01 C 1.0 -0.1 0.4 97.3 36.0 69.4
            // -99.000 -99.000 -99.000 -99.000 -99.000 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0
            String date = line.substring(6, 14);

            float temp_Max = Float.parseFloat(line.substring(39, 45).trim());
            float temp_Min = Float.parseFloat(line.substring(47, 53).trim());
            if (temp_Max > 40.0) {
                // Hot day
                context.write(new Text("Hot Day " + date), new Text(String.valueOf(temp_Max)));

            }
                if (temp_Min < 10) {
                // Cold day
                context.write(new Text("Cold Day " + date),
                new Text(String.valueOf(temp_Min)));
            }
        }
    }
    public static class MaxTemperatureReducer 
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text Key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Find Max temp yourself ?
            for (Text val : values) {
                context.write(Key,val);
            }
            
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "temp");
        // Note:- As Mapper's output types are not default so we have to define the following properties.
        job.setJarByClass(WeatherData.class);
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}