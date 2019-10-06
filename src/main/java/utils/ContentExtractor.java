package utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
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

public class ContentExtractor extends Configured implements Tool {

    public static class DocumentMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text document, Context context) throws IOException, InterruptedException {
            String docIdsStr = context.getConfiguration().get("relevant_doc_ids");
            ArrayList<String> relevantDocIdsList;
            relevantDocIdsList = new ArrayList<>();
            try {
                relevantDocIdsList = (ArrayList<String>) CustomSerializer.fromString(docIdsStr);

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            JSONObject jsonObject = new JSONObject(document.toString());
            String docId = jsonObject.get("id").toString();
            boolean relevant = relevantDocIdsList.contains(docId);
            if (relevant) {
                context.write(new Text(docId), document);
            }

        }

    }

    public static class DocumentReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Mapper.Context context
        ) throws IOException, InterruptedException {
            Text doc = values.iterator().next();
            context.write(key, doc);
        }
    }


    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), "content extractor");
        ArrayList<String> relIDs = new ArrayList<>();
        relIDs.add("12");
        relIDs.add("25");

        job.getConfiguration().set("relevant_doc_ids", CustomSerializer.toString(relIDs));
        job.setJarByClass(ContentExtractor.class);
        job.setMapperClass(DocumentMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(Paths.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(Paths.TEST));
        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int resultOfJob = ToolRunner.run(new ContentExtractor(), args);
        System.exit(resultOfJob);
    }
}
