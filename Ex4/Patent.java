package in.edureka.mapreduce;
import java.io.IOException;

import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Patent {

	public static class TokenizerMapper
			extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException
		{
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			
			Text keys = new Text(tokenizer.nextToken());
			context.write(keys, new IntWritable(1));
			while (tokenizer.hasMoreTokens())
			{
				Text temp = new Text(tokenizer.nextToken());
			}
		}
	}

	public static class IntSumReducer
			extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
						   Context context
		) throws IOException, InterruptedException {

			int sum = 0;

			while (values.iterator().hasNext())
			{
				sum += values.iterator().next().get();
			}
			System.out.println(key);
			context.write(key, new IntWritable(sum));
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Patent");
		job.setJarByClass(Patent.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
