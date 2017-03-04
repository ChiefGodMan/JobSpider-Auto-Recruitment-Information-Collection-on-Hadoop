/**
 * Created by ailias on 7/14/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class WordFrequencyReduder extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
    public static String columnFamily = "wordfrequency";
    Configuration conf = null;
    HTable wordFrequencyTable = null;
    Get get = null;
    Result result = null;

    public void setup(Context context) throws IOException, InterruptedException {
        conf = HBaseConfiguration.create();
        wordFrequencyTable = new HTable(conf, "WordFrequency");
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable one : values) {
            count += one.get();
        }
        //read the key's count from hbase and update it back.
        get = new Get(Bytes.toBytes(key.toString()));
        result = wordFrequencyTable.get(get);
        if (!result.isEmpty()) {//if the result is not empty, then add the value to count
            for (KeyValue rowKV : result.raw()) {
                count += Integer.parseInt(new String(rowKV.getValue()));
            }
        }
        Put put = new Put(key.toString().getBytes());
        put.addColumn(columnFamily.getBytes(), columnFamily.getBytes(), Integer.toString(count).getBytes());
        context.write(new ImmutableBytesWritable(key.toString().getBytes()), put);
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        wordFrequencyTable.close();
    }
}
