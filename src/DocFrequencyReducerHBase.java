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
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by ailias on 5/26/16.
 */
public class DocFrequencyReducerHBase extends TableReducer<Text, Text, ImmutableBytesWritable> {

    private static String columnFamily = "docfrequency";
    private double docfreq = 0;
    Configuration conf = null;
    HTable docFrequencyTable = null;
    Get get = null;
    Result result = null;

    public void setup(Context context) throws IOException, InterruptedException {
        conf = HBaseConfiguration.create();
        docFrequencyTable = new HTable(conf, "DocFrequency");
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //the input is : <item,path> ,output is : <item,docfrequency>
        HashSet<String> docNameHash = new HashSet<String>();
        for (Text value : values) {
            docNameHash.add(value.toString());
        }
        //read the key' count value from hbase and update it back
        get = new Get(Bytes.toBytes(key.toString()));
        result = docFrequencyTable.get(get);
        if (!result.isEmpty()) {//if the key result is not empty, then add the value.
            for (KeyValue rowKV : result.raw()) {
                docfreq = docNameHash.size() + Integer.parseInt(new String(rowKV.getValue()));
            }
        } else//first added doc
            docfreq = docNameHash.size();

        String docfreqstr = Double.toString(docfreq);//construct the output value string
        Put put = new Put(key.toString().getBytes());
        put.addColumn(columnFamily.getBytes(), columnFamily.getBytes(), docfreqstr.getBytes());
        context.write(new ImmutableBytesWritable(key.toString().getBytes()), put);
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        docFrequencyTable.close();
    }

}