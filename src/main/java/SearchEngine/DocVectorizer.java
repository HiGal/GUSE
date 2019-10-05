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
        public static class Vectorizer_Reduce extends Reducer<Text,Text,Text, ArrayWritable> {
            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                ArrayWritable array = new ArrayWritable(Text.class);
                List<String> array_list = new ArrayList<String>();
                for (Text val: values){
                    array_list.add(val.toString());
                }
                Text[] new_array= new Text[array_list.size()];
                for (int j = 0; j < array_list.size() ; j++) {
                    new_array[j]=new Text(array_list.get(j));
                }
                array.set(new_array);
                context.write(key, array);
            }
        }
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), "indexer");
        job.setJarByClass(DocVectorizer.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(Vectorizer_Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ArrayWritable.class);
        FileInputFormat.addInputPath(job, new Path("output/indexer/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("output/vectorizer"));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
