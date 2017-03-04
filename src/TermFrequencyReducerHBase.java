import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by ailias on 5/26/16.
 */
public class TermFrequencyReducerHBase extends TableReducer<Text, Text, ImmutableBytesWritable> {

    //private Text outputValue = new Text();
    private static String columnFamily = "termfrequency";

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int times = 0;//the item frequency in the doc
        for (Text value : values) {
            times += Integer.parseInt(value.toString());//sum the times of all addresses
        }
        Put put = new Put(key.toString().getBytes());
        put.addColumn(columnFamily.getBytes(), columnFamily.getBytes(), Integer.toString(times).getBytes());
        context.write(null, put);
        //context.write(new ImmutableBytesWritable(key.getBytes()), put);
    }

}