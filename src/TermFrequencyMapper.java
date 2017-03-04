import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by ailias on 5/26/16.
 */
public class TermFrequencyMapper extends Mapper<Object, Text, Text, Text> {

    private Text outputKey = new Text();//the output key
    private Text outputValue;//the output value treemap struct

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //the input is stringline, the output is : <word+path,1+addres>
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String name = inputSplit.getPath().getName();

        if (value.getLength() > 0) {//filtering the space string
            StringTokenizer stringTokens = new StringTokenizer(value.toString(), WordSegmentationMain.dilimt);
            String word;// item word
            int one = 1;
            while (stringTokens.hasMoreTokens()) {
                word = stringTokens.nextToken().toLowerCase();
                if (!CommonStaticClass.stopWordsHS.contains(word)) {//check is the word is stop word.
                    outputKey.set(word + "&" + name);//each item belong a <key,value> pair
                    outputValue = new Text(Integer.toString(one));
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

}
