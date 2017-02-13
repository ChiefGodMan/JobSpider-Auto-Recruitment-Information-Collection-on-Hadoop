import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by ailias on 7/16/16.
 */
public class NGram {

    public static TreeMap<String, Double> andCountMap = new TreeMap<String, Double>();//used for addcount result cache to accelerate
    public static TreeMap<String, Double> preCountMap = new TreeMap<String, Double>();//used for precount result cache to accelerate

    /**
     * get the specified table's total raw count, used in bm25 ranking algorithm
     *
     * @param conf
     * @return
     * @throws IOException
     */

    public static long getRowCount(Configuration conf) throws IOException {
//        String coprocessorClassName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
//        HBaseAdmin admin = new HBaseAdmin(conf);
//        admin.disableTable(tableName);
//        HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(tableName));
//        htd.addCoprocessor(coprocessorClassName);
//        admin.modifyTable(Bytes.toBytes(tableName), htd);
//        admin.enableTable(tableName);
        AggregationClient ac = new AggregationClient(conf);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(WordSegmentationMain.columnFamily));
        long rowCount = 0;

        try {
            HTable table = new HTable(conf, WordSegmentationMain.tableName);
            rowCount = ac.rowCount(table, new LongColumnInterpreter(), scan);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return rowCount;
    }

    /**
     * 根据给定的切分完成的句子，计算其联合概率，最终选择概率最大的作为分词结果
     * Using the maximum likelihood evaluation(MLE) method to calculate the sentence probability.
     *
     * @param words     one of segmented result for calculating the n-gram probability
     * @param wordTable
     * @return
     * @throws IOException
     */
    public static double getMLE(List<String> words, HTable wordTable) throws IOException {
        Get get;
        Scan scan;
        RegexStringComparator keyRegex;
        RowFilter rowFilter;
        ResultScanner resultScanner;
        double prob = 1.0;
        for (int i = 0; i < words.size() - 1; i++) {
            String wkey = words.get(i) + "&" + words.get(i + 1);
            double andcount = 0, precount = 0;
            if (andCountMap.containsKey(wkey)) {//if andcountMap exist the wkey, return the value directly
                andcount = andCountMap.get(wkey);
            } else {//if not exist the wkey, search in the hbase and add to the andcountMap for next reuse.
                get = new Get(wkey.getBytes());
                Result result = wordTable.get(get);
                if (result.size() > 0) {
                    for (KeyValue kv : result.list()) {
                        //String key = new String(kv.getRow());
                        andcount = Double.parseDouble(new String(kv.getValue()));
                        andCountMap.put(wkey, andcount);
                    }
                }
            }
            andcount += 1;//add 1 data smoothing
            if (preCountMap.containsKey(words.get(i))) {//if precountMap exist the words-, return the value directly
                precount = preCountMap.get(words.get(i));
            } else {//if not exist the wordsi, search in the hbase and add to the precountMap for next reuse.
                scan = new Scan();
                keyRegex = new RegexStringComparator(words.get(i) + "*");
                rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, keyRegex);
                scan.setFilter(rowFilter);
                resultScanner = wordTable.getScanner(scan);
                for (Result res : resultScanner) {
                    for (KeyValue kv : res.list()) {
                        //use the add-1 data smoothing method for data-sparse problem.
                        precount += Double.parseDouble(new String(kv.getValue()));
                    }
                }
                preCountMap.put(words.get(i), precount);
            }
            prob *= andcount / (precount + WordSegmentationMain.wordCount);
        }
        return prob;
    }

    /**
     * get all kinds of possible segmentation of the specified  sentence
     *
     * @param words     the input sentence of being processed
     * @param index     current processing sentence index
     * @param seg       current segmented part result.
     * @param segResult all segmented results of the sentence.
     */
    public static void getSegs(String words, int index, List<String> seg, List<List<String>> segResult) {
        for (int i = index + 1; i <= words.length() && (i-index)<=WordSegmentationMain.wordLen; i++) {
            String word = words.substring(index, i);
            List<String> tmpseg = new ArrayList<>(seg);
            if (CommonStaticClass.wordDict.contains(word) || word.matches("\\w+")) {// check if the word vocabulary belongs to the chinese dict or english/digits.
                if (word.matches("\\w+")) {//if the word is digit or english
                    while (word.matches("\\w+") && i < words.length()) {
                        word = words.substring(index, i++);
                    }
                    if (!word.matches("\\w+"))
                        i -= 2;
                        //i -=2;// why should I minus 2 index ???
                    word = words.substring(index, i);
                }
                tmpseg.add(word);
                if (i == words.length()) {
                    segResult.add(new ArrayList<>(tmpseg));
                    return;
                } else {
                    getSegs(words, i, tmpseg, segResult);
                }
            }
        }
    }


}
