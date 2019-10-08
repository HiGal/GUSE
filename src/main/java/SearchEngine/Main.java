package SearchEngine;

import org.apache.hadoop.util.ToolRunner;
import utils.InversedDocumentFrequency;
import utils.RelevanceAnalyzer;
import utils.TermFrequency;

public class Main {
    public static final String help = "Wrong arguments, help (arg1 arg2): \n ex1. Indexer /path/to input \n ex2. Query N 'query_string' ";
    public static void main(String[] args) throws Exception {
        if(args[0].equals("Indexer")) {
            if (args.length < 2){
                System.out.println(help);
                System.exit(-1);
            }
            ToolRunner.run(new TermFrequency(), args);
            ToolRunner.run(new InversedDocumentFrequency(), args);
            ToolRunner.run(new Indexer(), args);
            int vectorizer_res = ToolRunner.run(new DocVectorizer(), args);
            System.exit(vectorizer_res);
        }
        else if(args[0].equals("Query")) {
            if (args.length < 3){
                System.out.println(help);
                System.exit(-1);
            }
            int resultOfJob = ToolRunner.run(new RelevanceAnalyzer(), args);

            System.exit(resultOfJob);
        }else {
            System.out.println(help);
            System.exit(-1);
        }
    }
}
