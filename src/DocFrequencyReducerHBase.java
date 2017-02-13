import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by ailias on 5/26/16.
 */
public class DocFrequencyReducerHBase extends TableReducer<Text, Text, ImmutableBytesWritable> {

    private static String columnFamily = "docfrequency";
    private double docfreq = 0;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //the input is : <item,path> ,output is : <item,docfrequency>
        HashSet<String> docNameHash = new HashSet<String>();
        for (Text value : values) {
            docNameHash.add(value.toString());
        }
        if (WordSegmentationMain.docFrequencyMap.containsKey(key.toString()))//added new item's doc
            docfreq = docNameHash.size() + WordSegmentationMain.docFrequencyMap.get(key.toString());
        else//first added doc
            docfreq = docNameHash.size();
        String docfreqstr = Double.toString(docfreq);//construct the output value string
        Put put = new Put(key.toString().getBytes());
        put.addColumn(columnFamily.getBytes(), columnFamily.getBytes(), docfreqstr.getBytes());
        context.write(new ImmutableBytesWritable(key.toString().getBytes()), put);
    }

}