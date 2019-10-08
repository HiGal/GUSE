package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

public class ContentExtractor extends Configured implements Tool {
    private final static String JOB_NAME = "content extractor";
    private final static String REL_DOC_IDS = "relevant_doc_ids";
    private final static String JSON_FIELD_ID = "id";

    public static class DocumentMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text document, Context context) throws IOException, InterruptedException {
            JSONObject jsonObject = new JSONObject(document.toString());
            String docId = jsonObject.get(JSON_FIELD_ID).toString();
            ArrayList<String> relevantDocIdsList;
            try {
                relevantDocIdsList = (ArrayList<String>) CustomSerializer.fromString(context.getConfiguration().get(REL_DOC_IDS));
                boolean relevant = relevantDocIdsList.contains(docId);
                if (relevant) {
                    context.write(new Text(docId), document);
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }


        }

    }

    private ArrayList<String> readNRelevantIds(int N) throws IOException {
        ArrayList<String> result = new ArrayList<>();
        //Load file for IDF from vocabulary
        FileSystem fs = FileSystem.get(getConf());
        FSDataInputStream fileWithIDF = fs.open(new Path(Paths.CE_IDS));

        BufferedReader br = new BufferedReader(new InputStreamReader(fileWithIDF));

        String line = br.readLine();
        int iter = 0;
        while (line != null && iter < N) {
            StringTokenizer lines = new StringTokenizer(line, "\t");

            String id = lines.nextToken();
            result.add(id);

            line = br.readLine();
        }
        return result;
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), JOB_NAME);
        ArrayList<String> relIDs = readNRelevantIds(Integer.parseInt(args[0]));
        job.getConfiguration().set(REL_DOC_IDS, CustomSerializer.toString(relIDs));
        job.setJarByClass(ContentExtractor.class);
        job.setMapperClass(DocumentMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(Paths.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(Paths.QUERY_OUT));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int resultOfJob = ToolRunner.run(new ContentExtractor(), args);
        System.exit(resultOfJob);
    }
}
