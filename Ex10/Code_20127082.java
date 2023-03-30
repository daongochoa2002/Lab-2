package edu.ucr.cs.merlin;
import java.io.IOException;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
class Graph {
    private int V;
    private LinkedList<Integer>[] adj;
    ArrayList<ArrayList<Integer> > components
            = new ArrayList<>();

    @SuppressWarnings("unchecked") Graph(int v)
    {
        V = v;
        adj = new LinkedList[v];

        for (int i = 0; i < v; i++)
            adj[i] = new LinkedList();
    }

    void addEdge(int u, int w)
    {
        adj[u].add(w);
        adj[w].add(u);
    }

    void DFSUtil(int v, boolean[] visited,
                 ArrayList<Integer> al)
    {
        visited[v] = true;
        al.add(v);
        Iterator<Integer> it = adj[v].iterator();

        while (it.hasNext()) {
            int n = it.next();
            if (!visited[n])
                DFSUtil(n, visited, al);
        }
    }

    void DFS()
    {
        boolean[] visited = new boolean[V];

        for (int i = 0; i < V; i++) {
            ArrayList<Integer> al = new ArrayList<>();
            if (!visited[i]) {
                DFSUtil(i, visited, al);
                components.add(al);
            }
        }
    }
    int ConnecetedComponents() { return components.size(); }
}
public class Code_20127082 {
    public static boolean check = false;
    public static int dong = 0;
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString());
            IntWritable mother = new IntWritable(Integer.parseInt(itr.nextToken()));
            dong++;
            while (itr.hasMoreTokens())
            {
                check = true;
                context.write(new Text(String.valueOf(mother)), new IntWritable(Integer.parseInt(itr.nextToken())));
            }

            if (check == false)
            {
                context.write(new Text(String.valueOf(mother)), new IntWritable(-100));
            }
            check = false;
        }
    }
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, Text> {
        private IntWritable result = new IntWritable();
        public static ArrayList<ArrayList<Integer>> arr = new ArrayList<ArrayList<Integer>>();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            ArrayList<Integer> a = new ArrayList<>();
            for (IntWritable k : values)
            {
                if (k.get() >= 0)
                {
                    a.add(k.get());
                }
            }
                arr.add(a);

            if (arr.size() == dong)
            {
                Graph g = new Graph(dong);
                for (int i =0; i< arr.size(); i++)
                    for (int j = 0; j < arr.get(i).size(); j++)
                    {
                        g.addEdge(i, arr.get(i).get(j));
                    }
                g.DFS();
                context.write(new Text(""),new Text(String.valueOf(g.ConnecetedComponents())));
                System.out.println(g.ConnecetedComponents());
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Graph");
        job.setJarByClass(Code_20127082.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
