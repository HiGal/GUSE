package SearchEngine;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;
import utils.InversedTermFrequency;
import org.apache.hadoop.io.Text;
import utils.TermFrequency;

import javax.print.Doc;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;


public class Indexer extends Configured implements Tool {



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

    public static class TF_IDF_Reduce extends Reducer<Text, Text, Text, Text>{

        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            double idf = 0.0;
            List<String> cache = new ArrayList<String>();
            for(Text val: values){
                String[] tmp = val.toString().split(",");
                if (tmp[0].equals("")){
                    idf = Double.parseDouble(tmp[1]);
                }else {
                    cache.add(val.toString());
                }

            }

            for(String val1: cache){
                String[] tmp1 = val1.split(",");
                Text doc_id = new Text(tmp1[0]);
                double tfidf = Double.parseDouble(tmp1[1])*idf;
                context.write(doc_id,new Text("("+key+","+tfidf+")"));
            }

        }
    }

//    public static class Vectorizer_Reduce extends Reducer<Text,Text,Text, ArrayWritable>{
//        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            ArrayWritable array = new ArrayWritable(Text.class);
//            List<Text> array_list = new ArrayList<Text>();
//            int i = 0;
//            for (Text val: values){
//                array_list.add(val);
//            }
//            Text[] array1 = array_list.toArray(new Text[0]);
//
//            array.set(array1);
//            context.write(key, array);
//        }
//    }


    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), "indexer");
        job.setJarByClass(Indexer.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(TF_IDF_Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("output/tf/part-r-00000"));
        FileInputFormat.addInputPath(job, new Path("output/idf/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("output/indexer"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int tf_res = ToolRunner.run(new TermFrequency(), args);
        int idf_res = ToolRunner.run(new InversedTermFrequency(), args);
        int indexer_res = ToolRunner.run(new Indexer(), args);
        int vectorizer_res = ToolRunner.run(new DocVectorizer(),args);
        System.exit(vectorizer_res);
    }
}
