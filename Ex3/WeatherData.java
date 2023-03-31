import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WeatherData {
  public static class MaxTemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable arg0, Text Value,
        OutputCollector<Text, Text> output, Reporter arg3)
        throws IOException {
      String line = Value.toString();
      // Lay ngay/thang/nam
      String date = line.substring(6, 14);

      // Lay nhiet do cao nhat
      float temp_Max = Float.parseFloat(line.substring(39, 45).trim());

      // Lay nhiet do thap nhat
      float temp_Min = Float.parseFloat(line.substring(47, 53).trim());
      if (temp_Max > 40.0) {
        // Hot day
        output.collect(new Text(date), new Text("Hot Day" ));
      }
      if (temp_Min < 10) {
        // Cold day
        output.collect(new Text(date),new Text("Cold Day"));
      }
    }
  }


  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(WeatherData.class);
    conf.setJobName("temp");
    // Note:- As Mapper's output types are not default so we have to define the
    // following properties.
    conf.setMapperClass(MaxTemperatureMapper.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);
    
    
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    JobClient.runJob(conf);
  }
}