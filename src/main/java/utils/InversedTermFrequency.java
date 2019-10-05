package utils;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.lang.Math;

public class InversedTermFrequency extends Configured implements Tool {

    private final static String FILE_COUNT = "CountOfFiles";

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
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double fileCount = Double.parseDouble(String.valueOf(context.getConfiguration().get(FILE_COUNT)));
            int occuredDocCount = 0;
            for (DoubleWritable v : values) {
                occuredDocCount++;
            }
            result.set(Math.log(fileCount/occuredDocCount));
            context.write(key, result);
        }
    }

    public int getCountOfFiles() throws IOException {
        FileSystem fs = FileSystem.get(getConf());
        return fs.listStatus(new Path(Paths.INPUT_PATH)).length;
    }

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(getConf(), "idf");
        job.setJarByClass(InversedTermFrequency.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(Paths.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(Paths.IDF_OUT));
        Integer docCount = getCountOfFiles();
        job.getConfiguration().set(FILE_COUNT, docCount.toString());
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int resultOfJob = ToolRunner.run(new InversedTermFrequency(), args);
        System.exit(resultOfJob);
    }
}
