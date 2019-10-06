package utils;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import java.util.HashSet;
import java.util.StringTokenizer;

public class InversedTermFrequency extends Configured implements Tool {



    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private final DoubleWritable one = new DoubleWritable(1.0);


        public void map(Object key, Text document, Context context) throws IOException, InterruptedException {
            JSONObject json = new JSONObject(document.toString());
            Text content = new Text(json.get("text").toString());
            StringTokenizer words = new StringTokenizer(content.toString(), " \'\n.,!?:()[]{};\\/\"*");
            HashSet<String> procedeed = new HashSet<String>();
            while (words.hasMoreTokens()) {
                String word = words.nextToken().toLowerCase();
                if (word.equals("") || procedeed.contains(word)) {
                    continue;
                }
                procedeed.add(word);
                context.write(new Text(word), one);
            }
        }
    }

    public static class Reduce
            extends Reducer<Text, DoubleWritable, Text, Text> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int occuredDocCount = 0;
            for (DoubleWritable v : values) {
                occuredDocCount++;
            }
            result.set(occuredDocCount);
            context.write(new Text(key), new Text(","+result));
        }
    }


    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(getConf(), "idf");
        job.setJarByClass(InversedTermFrequency.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(Paths.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(Paths.ITF_OUT));
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
