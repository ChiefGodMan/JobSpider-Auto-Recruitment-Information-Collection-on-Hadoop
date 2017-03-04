/**
 * Created by ailias on 7/14/16.
 */

import java.io.IOException;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WordSegmentationMapper extends Mapper<Object, Text, Text, Text> {
    public static Configuration conf;
    public static HTable wordTable;
    public String fileName;
    StringTokenizer stringTokens;
    String segResultStr;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        wordTable = new HTable(conf, WordSegmentationMain.tableName);
        fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        if (!value.toString().trim().equals("")) {
            stringTokens = new StringTokenizer(value.toString(), WordSegmentationMain.dilimt);
            segResultStr = key.toString() + "&";
            while (stringTokens.hasMoreTokens()) {
                int startIndex = 0;// current index for recursive
                String words = stringTokens.nextToken().trim();
                if (words.matches("\\w+"))
                    segResultStr += " " + words;
                else {
                    //process english string part
                    if (words.substring(0, 1).matches("\\w+")) {// the words string begining is the characters
                        int index = 2;
                        while (words.substring(0, index).matches("\\w+") && index < words.length()) {
                            index++;
                        }
                        if (!words.substring(0, index).matches("\\w+"))
                            index -= 1;
                        segResultStr += " " + words.substring(0, index);
                        words = words.substring(index, words.length());
                    }

                    //process chinese string part.
                    int wordSplitedIndex = 0;
                    for (int wordi = 0; wordi < Math.ceil((double) words.length() / (double) WordSegmentationMain.sentenceLen); wordi++) {
                        ArrayList<String> seg = new ArrayList();//current segmentation result for recursive.
                        ArrayList<List<String>> segResult = new ArrayList<>();//the segmentation result array.
                        wordSplitedIndex = wordi * WordSegmentationMain.sentenceLen;
                        int sentenceLen = words.length() - wordSplitedIndex > WordSegmentationMain.sentenceLen ? WordSegmentationMain.sentenceLen : words.length() - wordSplitedIndex;
                        String newWords = words.substring(wordSplitedIndex, wordSplitedIndex + sentenceLen);
                        //get the words segmentation
//                        Date seg_start_time = new Date();
                        NGram.getSegs(newWords, startIndex, seg, segResult);
                        //get the result index in segResult map.
                        TreeMap<Double, Integer> probIndexMap = new TreeMap<>();//set the <prob,index> map value
                        for (int i = 0; i < segResult.size(); i++) {
                            probIndexMap.put(NGram.getMLE(segResult.get(i), wordTable), i);
                        }
                        if (probIndexMap.size() > 0) {
                            List<String> segRes = segResult.get(probIndexMap.lastEntry().getValue());
                            segResultStr += " " + String.join(" ", segRes);
                        }
//                        Date seg_stop_time = new Date();
//                        System.out.format("seg&mle-time:%d\n", seg_stop_time.getTime() - seg_start_time.getTime());
                    }
                }
            }
            context.write(new Text(fileName), new Text(segResultStr));
        }
    }

}
