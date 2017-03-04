/**
 * Created by ailias on 7/14/16.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class WordSegmentationReduder extends Reducer<Text, Text, NullWritable, Text> {
    private MultipleOutputs<NullWritable, Text> multipleOutputs;// used to output multiple files

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String segStr = "";
        TreeMap<Integer, String> SegMap = new TreeMap<>();
        for (Text sentence : values) {
            String[] SegArr = sentence.toString().split("&");
            if (SegArr.length > 1)
                SegMap.put(Integer.parseInt(SegArr[0]), SegArr[1]);
        }
        for (Map.Entry tm : SegMap.entrySet()) {
            segStr += " " + tm.getValue();
        }
        //context.write(key, new Text(segStr));
        multipleOutputs.write(NullWritable.get(), new Text(segStr), key.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //Only after close the object then the output can write to hdfs immediately
        multipleOutputs.close();
    }
}
