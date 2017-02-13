/**
 * Created by ailias on 7/14/16.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class WordStatisticsMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text outputKey;
    //private Text outputValue = new Text();
    private IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer stringTokens = new StringTokenizer(value.toString(), WordSegmentationMain.dilimt);
        while (stringTokens.hasMoreTokens()) {
            String words = stringTokens.nextToken();
            for (int i = 0; i < words.length() - WordSegmentationMain.wordLen; i++) {
                for (int j = i + 1; j < i + 1 + WordSegmentationMain.wordLen; j++) {
                    String prefWord = words.substring(i, j);
                    if (CommonStaticClass.wordDict.contains(prefWord)) {
                        for (int k = j + 1; k < words.length() && k < j + 1 + WordSegmentationMain.wordLen; k++) {
                            String lastWord = words.substring(j, k);
                            if (CommonStaticClass.wordDict.contains(lastWord)) {
                                String keyw = prefWord + "&" + lastWord;
                                outputKey = new Text(keyw.getBytes());
                                context.write(outputKey, one);
                            }
                        }
                    }
                }
            }
        }
    }

}
