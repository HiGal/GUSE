package utils;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.json.JSONObject;

import javax.naming.Context;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ContentExtractor extends Configured implements Tool {

    public static class DocumentMapper extends Mapper<Object, Text, BooleanWritable, Text> {

        private final BooleanWritable TRUE = new BooleanWritable(true);
        public void map(Object key, Text document, Context context) throws IOException, InterruptedException {
            String docIdsStr = context.getConfiguration().get("doc_ids");
            try {
                ArrayList docIdsList = (ArrayList) CustomSerializer.fromString(docIdsStr);

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            JSONObject jsonObject = new JSONObject(document.toString());
//            Integer DOC_ID = context.get
        }

    }
    public static class Reduce
            extends Reducer<Text, DoubleWritable, Text, Text> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            String[] splitted_key = key.toString().split(",");
            Text new_key = new Text(splitted_key[0]);
            result.set(sum);
            Text res = new Text(splitted_key[1]+","+result);
            context.write(new_key, res);
        }
    }
    public int run(String[] strings) throws Exception {
        return 0;
    }
}
