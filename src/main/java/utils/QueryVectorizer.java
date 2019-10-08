package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;

import java.util.*;

public class QueryVectorizer {

    /**
     * Return json in format {word:tf/idf} for each word in query
     * @param args - args contain query string
     * @param configuration - conf to open hdfs
     * @return json formatted string
     */
    public static String queryToVector(String[] args, Configuration configuration) throws Exception {

        Map<String, Double> queryVector = new HashMap<String, Double>();

        //Get text query (last argument in args)
        String query = args[args.length - 1].toLowerCase();
        StringTokenizer queryWords = new StringTokenizer(query, " \'\n.,!?:()[]{};\\/\"*");

        //Calculate the QF for each word in the query and put to the map
        while (queryWords.hasMoreTokens()) {
            String word = queryWords.nextToken();
            if (queryVector.containsKey(word)) {
                queryVector.put(word, queryVector.get(word) + 1.0);
            } else {
                queryVector.put(word, 1.0);
            }

        }


        //Load file for IDF from vocabulary
        FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream fileWithIDF = fs.open(new Path(Paths.IND_IN2));
        ;
        BufferedReader br = new BufferedReader(new InputStreamReader(fileWithIDF));

        // For each pair (word, idf)
        String line = br.readLine();
        while (line != null) {
            StringTokenizer lines = new StringTokenizer(line, "\t");

            String word = lines.nextToken();
            String idf = lines.nextToken();
            //If word in map, update value in map
            if (queryVector.containsKey(word)) {
                queryVector.put(word, queryVector.get(word) / Double.parseDouble(idf.substring(1)));
            }
            line = br.readLine();
        }

        String result = "";
        for (String key : queryVector.keySet()) {
            String value = queryVector.get(key).toString();
            result = result + ",\"" + key + "\":\"" + value + "\"";
        }
        result = "{" + result.substring(1) + "}";

        return (result);
    }

}