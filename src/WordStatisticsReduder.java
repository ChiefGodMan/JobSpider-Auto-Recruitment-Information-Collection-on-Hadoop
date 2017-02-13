/**
 * Created by ailias on 7/14/16.
 */

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class WordStatisticsReduder extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
    public static String columnFamily = "word";

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable one : values) {
            count += one.get();
        }
        Put put = new Put(key.toString().getBytes());
        put.addColumn(columnFamily.getBytes(), columnFamily.getBytes(), Integer.toString(count).getBytes());
        context.write(new ImmutableBytesWritable(key.toString().getBytes()), put);
    }
}
