import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ailias on 5/26/16.
 */
public class IndexingSortReducerHBase extends Reducer<Text, Text, Text,Text> {

    private Text outputValue = new Text();
    private static String columnFamily = "";

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String itemPath[] = key.toString().split(" ");//<item,path>pair
        int times = 0;//the item frequency in the doc


    }

}