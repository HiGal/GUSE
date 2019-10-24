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
import org.json.JSONObject;

/**
 * For each document calculate number of occurrences of each word
 */
public class TermFrequency extends Configured implements Tool {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private final DoubleWritable one = new DoubleWritable(1.0);

        /**
         * Map function splits text into words and creates tuples ((word, doc_id) 1) for each word
         *
         * @param key      - default key
         * @param document - json doc
         * @param context  - store
         */
        public void map(Object key, Text document, Context context) throws IOException, InterruptedException {
            JSONObject json = new JSONObject(document.toString());
            Text content = new Text(json.get("text").toString());
            String str = json.get("title") .toString().replaceAll(","," ");
            String doc_id = json.get("id").toString() + " " +str;

            StringTokenizer words = new StringTokenizer(content.toString(), " \'\n.,!?:()[]{};\\/\"*");
            while (words.hasMoreTokens()) {
                String word = words.nextToken().toLowerCase();
                if (word.equals("")) {
                    continue;
                }
                context.write(new Text(word + "," + doc_id), one);
            }
        }
    }

    public static class Reduce
            extends Reducer<Text, DoubleWritable, Text, Text> {
        private DoubleWritable result = new DoubleWritable();

        /**
         * Reduce function sums up array of ones for each unique (word, doc_id) key
         * and puts (word, (doc_id, result)) tuples into store
         *
         * @param key     - (word,doc_id) string
         * @param values  - array of ones
         * @param context - store
         */
        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            String[] splitted_key = key.toString().split(",");
            Text new_key = new Text(splitted_key[0]);
            result.set(sum);
            Text res = new Text(splitted_key[1] + "," + result);
            context.write(new_key, res);
        }
    }


    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(getConf(), "word count");
        job.setJarByClass(TermFrequency.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(Paths.TF_OUT));
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
