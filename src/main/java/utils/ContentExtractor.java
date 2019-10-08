package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class ContentExtractor extends Configured implements Tool {
    private final static String JOB_NAME = "content extractor";
    private final static String REL_DOC_IDS = "relevant_doc_ids";
    private final static String JSON_FIELD_ID = "id";

    public static class DocumentMapper extends Mapper<Object, Text, IntWritable, Text> {
        /**
         * Map returns relevant documents found where key is their ID
         *
         * @param key      default key
         * @param document document text JSON
         * @param context  store ID , document
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text document, Context context) throws IOException, InterruptedException {
            JSONObject jsonObject = new JSONObject(document.toString());
            String docId = jsonObject.get(JSON_FIELD_ID).toString();
            ArrayList<String> relevantDocIdsList;
            try {
                relevantDocIdsList = (ArrayList<String>) CustomSerializer.fromString(context.getConfiguration().get(REL_DOC_IDS));
                boolean relevant = relevantDocIdsList.contains(docId);
                if (relevant) {
                    context.write(new IntWritable(Integer.parseInt(docId)), document);
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Reads list of N relevant id strings
     *
     * @param N number of documents
     * @return list of ids
     * @throws IOException
     */
    private ArrayList<String> readNRelevantIds(int N) throws IOException {
        ArrayList<String> result = new ArrayList<>();
        //Load file for IDF from vocabulary
        FileSystem fs = FileSystem.get(getConf());
        FSDataInputStream fileWithIDRank = fs.open(new Path(Paths.CE_IDS));

        BufferedReader br = new BufferedReader(new InputStreamReader(fileWithIDRank));

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

    /**
     * Runs the MapReduce
     *
     * @param args 1 parameter N, number of relevant IDs to be shown
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        int N = Integer.parseInt(args[0]);
        Job job = Job.getInstance(getConf(), JOB_NAME);
        ArrayList<String> relIDs = readNRelevantIds(N);
        job.getConfiguration().set(REL_DOC_IDS, CustomSerializer.toString(relIDs));
        job.setJarByClass(ContentExtractor.class);
        job.setMapperClass(DocumentMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileSystem fs = FileSystem.get(getConf());
        fs.delete(new Path(Paths.QUERY_OUT), true);

        FileInputFormat.addInputPath(job, new Path(Paths.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(Paths.QUERY_OUT));
        int completion = job.waitForCompletion(true) ? 0 : 1;
        printIDRankTitle(N);
        return completion;
    }

    /**
     * Returns HashMap of ID:JSON of extracted documents
     *
     * @return HashMap
     * @throws IOException
     */
    private HashMap<Integer, JSONObject> readJSONs() throws IOException {
        HashMap<Integer, JSONObject> result = new HashMap<>();

        FileSystem fs = FileSystem.get(getConf());
        FSDataInputStream fileWithIDJSON = fs.open(new Path(Paths.ID_JSON));
        BufferedReader brJSON = new BufferedReader(new InputStreamReader(fileWithIDJSON));
        String lineJSON = brJSON.readLine();
        while (lineJSON != null) {
            StringTokenizer linesJSON = new StringTokenizer(lineJSON, "\t");
            int id = Integer.parseInt(linesJSON.nextToken());
            JSONObject json = new JSONObject(linesJSON.nextToken());
            result.put(id, json);

            lineJSON = brJSON.readLine();
        }
        return result;
    }

    /**
     * Prints document ID, title and its rank to the console
     *
     * @param N number of relevant documents
     * @throws IOException
     */
    private void printIDRankTitle(int N) throws IOException {
        //Load file for IDF from vocabulary
        FileSystem fs = FileSystem.get(getConf());
        FSDataInputStream fileWithIDRank = fs.open(new Path(Paths.CE_IDS));
        BufferedReader brRank = new BufferedReader(new InputStreamReader(fileWithIDRank));

        HashMap<Integer, JSONObject> JSONMap = readJSONs();

        String lineRank = brRank.readLine();
        int iter = 0;
        while (lineRank != null && iter < N) {
            StringTokenizer linesRank = new StringTokenizer(lineRank, "\t");

            int id = Integer.parseInt(linesRank.nextToken());
            String rank = linesRank.nextToken();


            String title = (String) JSONMap.get(id).get("title");
            System.out.printf("ID:%5s | Title:%10s | Rank:%5s\n", id, title, rank);

            lineRank = brRank.readLine();
            iter++;
        }
    }

    /**
     * Entrypoint
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        int resultOfJob = ToolRunner.run(new ContentExtractor(), args);

        System.exit(resultOfJob);
    }
}
