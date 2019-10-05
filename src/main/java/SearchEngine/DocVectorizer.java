package SearchEngine;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class DocVectorizer extends Configured implements Tool {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        public final DoubleWritable one = new DoubleWritable(1.0);

        public void map(Object key, Text document, Context context) throws IOException, InterruptedException {
            StringTokenizer words = new StringTokenizer(document.toString(), "\t");
            while (words.hasMoreTokens()) {
                String word = words.nextToken();
                Text residual = new Text(words.nextToken());
                context.write(new Text(word), residual);
            }
        }
    }
        public static class Vectorizer_Reduce extends Reducer<Text,Text,Text, Text> {
            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

                JSONObject json = new JSONObject();
                for (Text val: values){
                    String[] tmp = val.toString().split(",");
                    json.put(tmp[0],tmp[1]);
                }


                context.write(key, new Text(json.toString()));
            }
        }
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), "indexer");
        job.setJarByClass(DocVectorizer.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(Vectorizer_Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("output/indexer/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("output/vectorizer"));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
