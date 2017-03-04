import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by ailias on 5/26/16.
 */
public class DocFrequencyMapper extends Mapper<Object, Text, Text, Text> {

    private Text outputKey = new Text();//the output key
    private Text outputValue = new Text();//the output value treemap struct

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //the input is stringline,the output is <item,path>
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String name = inputSplit.getPath().getName();

        if (value.getLength() > 0) {//filtering the space string
            StringTokenizer stringTokens = new StringTokenizer(value.toString(), WordSegmentationMain.dilimt);
            while(stringTokens.hasMoreTokens()){
                String word = stringTokens.nextToken().toLowerCase();
                if(!CommonStaticClass.stopWordsHS.contains(word)){
                    outputKey.set(word);
                    outputValue.set(name);
                    context.write(outputKey, outputValue);
                }
            }

        }
    }


}
