public class QueryVectorizer{
    public static Map<String, Double> query_to_vector(String[] args) throws Exception {

        Map<String, Double> queryVector = new HashMap<String, Integer>();

        //Get text query (last argument in args)
        String query = args[args.length-1];
        String[] queryWords = query.split(" ");
        int wordsNum = queryWords.length;

        //Culculate the QF for each word in the query and put to the map
        for(String word : queryWords){
            if(queryVector.containsKey(word)){
                queryVector.put(word, map.get(word) + 1/wordsNum);
            }
        }


//TODO Load file for IDF from vocadulary
        Configuration conf = new Configuration();

//TODO check if it the correct configs to open files in hadoop
        conf.addResource(new Path("/hadoop/projects/hadoop-1.0.4/conf/core-site.xml"));
        conf.addResource(new Path("/hadoop/projects/hadoop-1.0.4/conf/hdfs-site.xml"));

        Path path = new Path("output/idf/part-r-00000");
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream fileWithIDF = fs.open(path);
        StringTokenizer lines = new StringTokenizer(fileWithIDF.toString(), "\t");

        while (lines.hasMoreTokens()) {
            //read each pair of (word,idf)
            String word = words.nextToken();
            String idf = words.nextToken();

            //if word in mup, update value in map
            if(queryVector.containsKey(word)){
                queryVector.put(word, map.get(word) * Double.parseDouble(idf);
            }
        }
        return(queryVector);
    }

}