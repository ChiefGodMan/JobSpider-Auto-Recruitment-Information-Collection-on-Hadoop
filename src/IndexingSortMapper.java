import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by ailias on 5/26/16.
 */
public class IndexingSortMapper extends TableMapper<Text,Text> {

    private Text outputKey = new Text();//the output key
    private Text outputValue = new Text();//the output value treemap struct
    private static String columnFamily = "termfrequency";

    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {


    }

}
