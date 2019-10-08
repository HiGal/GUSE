package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class ContentExtractor {
    private final static java.lang.String JOB_NAME = "content extractor";
    private final static java.lang.String REL_DOC_IDS = "relevant_doc_ids";
    private final static java.lang.String JSON_FIELD_ID = "id";

    /**
     * Reads list of N relevant id strings
     *
     * @param N number of documents
     * @return list of ids
     * @throws IOException
     */
    private static ArrayList<java.lang.String> readNRelevantIds(int N, org.apache.hadoop.conf.Configuration c) throws IOException {
        ArrayList<java.lang.String> result = new ArrayList<>();
        //Load file for IDF from vocabulary
        FileSystem fs = FileSystem.get(c);
        FSDataInputStream fileWithIDRank = fs.open(new Path(Paths.CE_IDS));

        BufferedReader br = new BufferedReader(new InputStreamReader(fileWithIDRank));

        java.lang.String line = br.readLine();
        int iter = 0;
        while (line != null && iter < N) {
            result.add(line);
            iter++;
            line = br.readLine();
        }
        return result;
    }

    /**
     * Runs the MapReduce
     *
     * @param args 1 parameter N, arg 2 number of relevant IDs to be shown
     */
    public static int run(java.lang.String[] args, org.apache.hadoop.conf.Configuration c) throws Exception {
        int N = Integer.parseInt(args[1].toString());
        ArrayList<java.lang.String> res = readNRelevantIds(N, c);
        System.out.println("ID  |   Title   | Rank");
        for (java.lang.String r : res) {
            System.out.println(r);
        }
        return 0;
    }

}
