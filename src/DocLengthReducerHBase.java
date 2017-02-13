import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by ailias on 5/26/16.
 */
public class DocLengthReducerHBase extends TableReducer<Text, Text, ImmutableBytesWritable> {

    private static String columnFamily = "doclength";

    public void reduce(Text key, Iterable<Integer> values, Context context) throws IOException, InterruptedException {

        int times = 0;//the item frequency in the doc
        for (Integer value : values) {
            times += value;//sum the times of all addresses
        }
        String outputTimes = Integer.toString(times);//construct the output value string
        Put put = new Put(key.toString().getBytes());
        put.addColumn(columnFamily.getBytes(), columnFamily.getBytes(), outputTimes.getBytes());
        context.write(new ImmutableBytesWritable(key.toString().getBytes()), put);
    }

}