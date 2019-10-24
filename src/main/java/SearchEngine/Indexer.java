package SearchEngine;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.InversedDocumentFrequency;
import org.apache.hadoop.io.Text;
import utils.Paths;
import utils.TermFrequency;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


/**
 * Create tuples (doc_id, (word, tf/idf)) for each unique word in document
 */
public class Indexer extends Configured implements Tool {


    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        public final DoubleWritable one = new DoubleWritable(1.0);

        /**
         * Map function merges inputs from two directories (tf, idf)
         *
         * @param key      - default key
         * @param document - either line from tf or idf output files
         * @param context  - store
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

    public static class TF_IDF_Reduce extends Reducer<Text, Text, Text, Text> {

        /**
         * Reduce function calculates tf/idf for each word, doc_id in values
         * and puts (doc_id, (word, tf/idf)) into result
         *
         * @param key     - some unique word
         * @param values  - idf and array of tf's for documents with this word
         * @param context - store
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double idf = 0.0;
            List<String> cache = new ArrayList<String>();
            for (Text val : values) {
                String[] tmp = val.toString().split(",");

                // determine type of input (tf - (doc_id comma number)/idf - just (number))
                if (tmp[0].equals("")) {
                    idf = Double.parseDouble(tmp[1]);
                } else {
                    cache.add(val.toString());
                }

            }

            for (String val1 : cache) {
                String[] tmp1 = val1.split(",");
                Text doc_id = new Text(tmp1[0]);
                // tf idf for word
                double tfidf = Double.parseDouble(tmp1[1]) / idf;
                context.write(doc_id, new Text(key + "," + tfidf));
            }

        }
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "indexer");
        job.setJarByClass(Indexer.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(TF_IDF_Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(Paths.IND_IN1));
        FileInputFormat.addInputPath(job, new Path(Paths.IND_IN2));
        FileOutputFormat.setOutputPath(job, new Path(Paths.IND_OUT));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Main Indexer job
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new TermFrequency(), args);
        ToolRunner.run(new InversedDocumentFrequency(), args);
        ToolRunner.run(new Indexer(), args);
        int vectorizer_res = ToolRunner.run(new DocVectorizer(), args);
        System.exit(vectorizer_res);
    }
}
