package utils;

import java.io.IOException;
import java.util.StringTokenizer;

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
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

public class TermFrequency extends Configured implements Tool {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private final  DoubleWritable one = new DoubleWritable(1.0);
        private Text word = new Text();

        public void map(Object key, Text document, Context context) throws IOException, InterruptedException {
            JSONObject json = new JSONObject(document.toString());
            Text content = new Text(json.get("text").toString());
            String doc_id = json.get("id").toString();
            StringTokenizer words = new StringTokenizer(content.toString(), " \'\n.,!?:()[]{};\\/\"*");
            int total_words_num = words.countTokens();
            while (words.hasMoreTokens()) {
                String word = words.nextToken().toLowerCase();
                if (word.equals("")) {
                    continue;
                }
                DoubleWritable freq = new DoubleWritable(one.get()/(double)total_words_num);
                context.write(new Text("("+word+","+doc_id+")"), freq);
            }
        }
    }

    public static class Reduce
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(getConf(), "word count");
        job.setJarByClass(TermFrequency.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path("EnWikiSubset"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int resultOfJob = ToolRunner.run(new TermFrequency(),args);
        System.exit(resultOfJob);
    }
}
