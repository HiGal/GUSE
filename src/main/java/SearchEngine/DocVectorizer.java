package SearchEngine;

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
import utils.Paths;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Create {word1:tf_idf1, word2:tf_idf2, ...} json-based vector for each document
 */
public class DocVectorizer extends Configured implements Tool {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        public final DoubleWritable one = new DoubleWritable(1.0);

        /**
         * Map function splits previous job (indexer) into key(doc_id) and value (word,tf_idf) result for new reduce
         *
         * @param key - default key
         * @param document - string in format doc_id   word,tf_idf
         * @param context - store
         */
        public void map(Object key, Text document, Context context) throws IOException, InterruptedException {
            StringTokenizer words = new StringTokenizer(document.toString(), "\t");
            while (words.hasMoreTokens()) {
                String word = words.nextToken();
                Text residual = new Text(words.nextToken());
                context.write(new Text(word), residual);
            }
        }
    }

    public static class Vectorizer_Reduce extends Reducer<Text, Text, Text, Text> {

        /**
         * Reduce function merges all words, tf_idf for same doc into json vector in format word:tf_idf
         * @param key - doc_id
         * @param values - array of <word,tf_ifd> strings
         * @param context - store
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            JSONObject json = new JSONObject();
            for (Text val : values) {
                String[] tmp = val.toString().split(",");
                json.put(tmp[0], tmp[1]);
            }
            context.write(key, new Text(json.toString()));
        }
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "indexer");
        job.setJarByClass(DocVectorizer.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(Vectorizer_Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(Paths.VECT_IN));
        FileOutputFormat.setOutputPath(job, new Path(Paths.VECT_OUT));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
