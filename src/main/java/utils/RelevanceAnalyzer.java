package utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

/**
 * Compute Relevance of Query to all documents
 */
public class RelevanceAnalyzer extends Configured implements Tool {

    public static final String QUERY = "query_text";

    public static class RelevanceMapper
            extends Mapper<Object, Text, DoubleWritable, Text> {

        /**
         * Map Function multiplies Query TF/IDF vector by each Document TF/IDF vector
         *
         * @param key      - default key
         * @param document - json document
         * @param context  - store (rank, doc_id) to allow shuffle stage automatically sort by rank
         */
        public void map(Object key, Text document, Context context) throws IOException, InterruptedException {
            StringTokenizer words = new StringTokenizer(document.toString(), "\t");
            Text docId = new Text(words.nextToken());
            String ww = words.nextToken();
            JSONObject docVect = new JSONObject(ww);
            JSONObject queryVec = new JSONObject(context.getConfiguration().get(QUERY));
            double res = 0;
            Iterator<String> keys = queryVec.keys();
            while (keys.hasNext()) {
                String qKey = keys.next();
                if (docVect.has(qKey))
                    res += Double.parseDouble(docVect.get(qKey).toString()) * Double.parseDouble(queryVec.get(qKey).toString());
            }
            context.write(new DoubleWritable(res * -1), new Text(docId));
        }
    }

    public static class RelevanceReducer
            extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

        /**
         * Reduce class just puts to store doc ids sorted by relevance (rank)
         *
         * @param Rank    - rank
         * @param values  - document id
         * @param context - store
         */
        public void reduce(DoubleWritable Rank, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Text docId = new Text(values.iterator().next().toString());
            context.write(docId, new DoubleWritable(Rank.get() * -1));
        }
    }


    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "relevance analyzer");
        job.setJarByClass(RelevanceAnalyzer.class);
        job.setMapperClass(RelevanceMapper.class);
        job.setReducerClass(RelevanceReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileSystem fs = FileSystem.get(getConf());
        Path out = new Path(Paths.RELV_OUT);
        if ((fs.exists(out) & !fs.delete(out, true)) | (fs.exists(new Path(Paths.QUERY_OUT)) & !fs.delete(new Path(Paths.QUERY_OUT), true))) {
            System.out.println("Remove already existing output directory (output/relevance, output/query-out) first, automatic remove failed");
            System.exit(-1);
        }
        FileInputFormat.addInputPath(job, new Path(Paths.RELV_IN1));
        FileOutputFormat.setOutputPath(job, new Path(Paths.RELV_OUT));
        job.getConfiguration().set(QUERY, QueryVectorizer.queryToVector(args, job.getConfiguration()));
        int k = job.waitForCompletion(true) ? 0 : 1;
        ContentExtractor.run(args, getConf());
        return k;
    }

    public static void main(String[] args) throws Exception {
        int resultOfJob = ToolRunner.run(new RelevanceAnalyzer(), args);
        System.exit(resultOfJob);
    }
}
