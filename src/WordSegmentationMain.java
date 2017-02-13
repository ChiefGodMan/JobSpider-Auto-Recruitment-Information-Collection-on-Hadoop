import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jruby.RubyBoolean;


/**
 * Created by ailias on 7/14/16.
 */

public class WordSegmentationMain {
    public final static int wordLen = 4;//specify the max length of one word
    public static List<String> inputFilePaths;
    public final static int sentenceLen = 20; //specify the max length of one sentence
    public static long wordCount;
    public final static String dilimt = " 、，：；。？！“”‘’《》（）【】￥—— .,;?/\\<>()[]{}!~`@#$%^&*_+-=:";
    public final static String tableName = "Word", columnFamily = "word";
    public final static String dictFileName = "files/dict/Dict.txt";
    public static int threadNum = 8;
    public static HTable wordTable;

    public static long docNum = 0;
    public static TreeMap<String, Double> docLengthMap = new TreeMap<String, Double>();//store <docpath,length>
    public static TreeMap<String, Double> docFrequencyMap = new TreeMap<String, Double>();//store <item,docfrequency>
    public static long avgDocLen = 0;
    public static long totDocLen = 0;//tottal docs lenght to compute the avgdoclen
    public final static double k1 = 1.5;
    public final static double b = 0.75;

    /**
     * 对于给定的训练预料统计其中的词语的次数用于计算其频率，该函数用于调用mapreduce任务
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void createWordFrequencyJob(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "WordFrequency");
        job.setJarByClass(WordSegmentationMain.class);
        job.setMapperClass(WordStatisticsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        TableMapReduceUtil.initTableReducerJob(WordSegmentationMain.tableName, WordStatisticsReduder.class, job);

        for (int i = 0; i < inputFilePaths.size(); i++) {
            FileInputFormat.addInputPath(job, new Path(inputFilePaths.get(i)));
        }
        job.waitForCompletion(true);
    }

    /**
     * create word segmentation job for all need segmentation file.
     *
     * @param conf
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void createWordSegmentationJob(Configuration conf, String outPath) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, "WordSegmentation");
        job.setJarByClass(WordSegmentationMain.class);
        job.setMapperClass(WordSegmentationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(WordSegmentationReduder.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < inputFilePaths.size(); i++) {
            FileInputFormat.addInputPath(job, new Path(inputFilePaths.get(i)));
        }
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        FileSystem fs = DistributedFileSystem.get(URI.create(outPath), conf);
        if (fs.exists(new Path(outPath))) {
            fs.delete(new Path(outPath), true);
        }

        job.waitForCompletion(true);
    }


    /**
     * calculate the doc length in each file to hbase for BM25 calculation
     * output the <docName,wordLen> pairs to hbase
     *
     * @param conf
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void calDocLength(Configuration conf, String path) throws IOException, InterruptedException, ClassNotFoundException {
        //1.this block code is used to count the document length
        String tableName = "DocLength";
        Job docLenjob = Job.getInstance(conf, "DocLength");
        docLenjob.setJarByClass(WordSegmentationMain.class);
        docLenjob.setMapperClass(DocLengthMapper.class);
        docLenjob.setMapOutputKeyClass(Text.class);
        docLenjob.setMapOutputValueClass(Integer.class);
        TableMapReduceUtil.initTableReducerJob(tableName, DocLengthReducerHBase.class, docLenjob);
        docLenjob.setNumReduceTasks(5);//reducer task's number
//        for (int i = 0; i < newFilePaths.size(); i++) {
//            FileInputFormat.addInputPath(docLenjob, new Path(newFilePaths.get(i)));
//        }
        FileInputFormat.addInputPath(docLenjob, new Path(path));
        docLenjob.waitForCompletion(true);
    }


    /**
     * calculate the docfrenquency for each item in each file for the BM25 calculation
     *
     * @param conf
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void calDocFrequency(Configuration conf, String path) throws IOException, InterruptedException, ClassNotFoundException {
        //2.this block code is used to count how many docs include this item
        String tableName = "DocFrequency";
        Job docFreqJob = Job.getInstance(conf, "DocFrequency");
        docFreqJob.setJarByClass(WordSegmentationMain.class);
        docFreqJob.setMapperClass(DocFrequencyMapper.class);
        docFreqJob.setMapOutputKeyClass(Text.class);
        docFreqJob.setMapOutputValueClass(Text.class);
        TableMapReduceUtil.initTableReducerJob(tableName, DocFrequencyReducerHBase.class, docFreqJob);
        docFreqJob.setNumReduceTasks(2);//reducer task's number
//        for (int i = 0; i < newFilePaths.size(); i++) {
//            FileInputFormat.addInputPath(docFreqJob, new Path(newFilePaths.get(i)));
//        }
        FileInputFormat.addInputPath(docFreqJob, new Path(path));
        docFreqJob.waitForCompletion(true);
    }


    /**
     * calculate the term frequency for all files and store the (item+path,times+addrs[]) to the hbase
     *
     * @param conf
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void calTermFrequency(Configuration conf, String path) throws IOException, InterruptedException, ClassNotFoundException {
        //3.this block code is used to count each item frequency in each document
        String tableName = "TermFrequency";
        Job termFreqJob = Job.getInstance(conf, "TermFrequency");
        termFreqJob.setJarByClass(WordSegmentationMain.class);
        termFreqJob.setMapperClass(TermFrequencyMapper.class);
        termFreqJob.setMapOutputKeyClass(Text.class);
        termFreqJob.setMapOutputValueClass(Integer.class);
        TableMapReduceUtil.initTableReducerJob(tableName, TermFrequencyReducerHBase.class, termFreqJob);
        termFreqJob.setNumReduceTasks(2);//reducer task's number
//        for (int i = 0; i < newFilePaths.size(); i++) {
//            FileInputFormat.addInputPath(termFreqJob, new Path(newFilePaths.get(i)));
//        }
        FileInputFormat.addInputPath(termFreqJob, new Path(path));
        termFreqJob.waitForCompletion(true);
    }

    /**
     * calculate the BM25 for the query.
     * First, scan the related record in hbase for join.
     * Second,calculate the BM25 ranking for each file
     * Last,sort the BM25 ranking as the result
     *
     * @param conf
     * @param DateDir
     * @param queries
     * @return
     * @throws IOException
     */
    public static NavigableMap<String, String> queryRanking(Configuration conf, String DateDir, String queries[]) throws IOException {
        //final sorting code
        String tableName = "TermFrequency";
        HTable queryTable = new HTable(conf, tableName);
        Scan queryScan = new Scan();
        ResultScanner resultScanner;
        HashMap<String, String> itemPathHM[] = new HashMap[queries.length];//used to store the scan result
        TreeMap<String, Integer> itemRankMP[] = new TreeMap[queries.length];//used to store the item ranking
        for (int i = 0; i < queries.length; i++) {//init map variable
            itemPathHM[i] = new HashMap();
            itemRankMP[i] = new TreeMap<>();
        }
        for (int i = 0; i < queries.length; i++) {//scan the hbase for each query parameter
            queryScan.setRowPrefixFilter((queries[i]).getBytes());
            resultScanner = queryTable.getScanner(queryScan);
            for (Result res : resultScanner) {//each query result
                for (KeyValue kv : res.list()) {//store the path and timesaddrs to itempathHM for the BM25 calculation
                    String itemPath = new String(kv.getRow());
                    String itempathArr[] = itemPath.split("&");
                    //add it to itempathHM.
                    String timesAddrs;
                    if (itemPathHM[i].containsKey(itempathArr[1])) {
                        String nextValue = new String(kv.getValue());
                        timesAddrs = itemPathHM[i].get(itempathArr[1]) + nextValue.split("&")[1];
                    } else {
                        timesAddrs = new String(kv.getValue());
                    }
                    itemPathHM[i].put(itempathArr[1], timesAddrs);

                }
            }
        }
        //next calculate the bm25 ranking values
        double doclen = 0;//document length
        double docfre = 0;//item's document frequency
        double tf = 0;
        double totbm25 = 0;
        double tmpbm25 = 0;
        String timesAddrs[];
        TreeMap<String, String> BM25 = new TreeMap<>();
        for (int i = 0; i < queries.length; i++) {//each query
            if (docFrequencyMap.containsKey(queries[i])) {//only there exist the query key ,then we can calculate the BM25 value.
                for (String path : itemPathHM[i].keySet()) {//get each query's path
                    doclen = docLengthMap.get(path);//get doc length
                    docfre = docFrequencyMap.get(queries[i]);//get doc frequency for one item
                    timesAddrs = itemPathHM[i].get(path).split("&");//get the item count in one file
                    tf = Double.parseDouble(timesAddrs[0]) / doclen;//calculate the term frequency
                    tmpbm25 = Math.log((double) docNum / docfre) * (k1 + 1) * tf / (k1 * ((1 - b) + b * doclen / avgDocLen) + tf);
                    String rankAddr[];
                    String addr = "";
                    if (BM25.containsKey(path)) {//sum the rank for all queries
                        rankAddr = BM25.get(path).split("&");//get the rank number in map
                        totbm25 = Double.parseDouble(rankAddr[0]) + tmpbm25;
                        addr = rankAddr[1];
                    } else
                        totbm25 = tmpbm25;
                    BM25.put(path, Double.toString(totbm25) + "&" + addr + timesAddrs[1]);
                }

            } else {
                System.out.format("Sorry, your input query string '%s' can not be found!\n", queries[i]);
            }
        }//end
        TreeMap<String, String> result = new TreeMap<>();
        String rankAddr[];
        for (String path : BM25.keySet()) {
            rankAddr = BM25.get(path).split("&");
            result.put(rankAddr[0] + "&" + path, rankAddr[1]);
        }
        return result.descendingMap();
    }

    /**
     * read all datas from hbase for BM25
     * process the init case
     *
     * @param conf
     * @throws IOException
     */
    public static void readAllDatasFromHBase(Configuration conf) throws IOException, Throwable {
        readAllDocLength(conf);
        docFrequencyMap = readAllDocFrequency(conf);
    }


    /**
     * read all docfrequency in 'DocFrequency' table both the first init and find new files
     *
     * @param conf
     * @return
     * @throws IOException
     */
    public static TreeMap<String, Double> readAllDocFrequency(Configuration conf) throws IOException {
        //read the DocFrequency hbase table to static docFrequencyMap variable
        TreeMap<String, Double> tmpdocFrequencyMap = new TreeMap<>();
        String docFreqTableName = "DocFrequency";
        HTable docFreqTable = new HTable(conf, docFreqTableName);
        Scan docFreqscan = new Scan();
        ResultScanner docFreqScanner = null;
        docFreqScanner = docFreqTable.getScanner(docFreqscan);
        for (Result docFreqRes : docFreqScanner) {
            for (KeyValue kv : docFreqRes.list()) {
                System.out.println(kv.getValue().toString());
                tmpdocFrequencyMap.put(kv.getRow().toString(), Double.parseDouble(kv.getValue().toString()));
            }
        }
        docFreqScanner.close();
        docFreqTable.close();
        return tmpdocFrequencyMap;
    }


    /**
     * read all doc length in 'DocLength' table ,just run one times
     *
     * @param conf
     * @throws IOException
     */
    public static void readAllDocLength(Configuration conf) throws IOException {
        String docLengthTableName = "DocLength";
        HTable docLengthTable = new HTable(conf, docLengthTableName);
        Scan docLengthScan = new Scan();
        ResultScanner docLengthScanner = null;
        docLengthScanner = docLengthTable.getScanner(docLengthScan);
        for (Result docLengthRes : docLengthScanner) {
            for (KeyValue kv : docLengthRes.list()) {
                double docl = Double.parseDouble(kv.getValue().toString());
                docLengthMap.put(kv.getRow().toString(), docl);
                totDocLen += docl;
            }
            docNum += 1;
        }
        if (docNum > 0)
            avgDocLen = totDocLen / docNum;
        docLengthScanner.close();
        docLengthTable.close();
    }


    public static void main(String[] args) throws Throwable {
        Scanner in = new Scanner(System.in);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        //String dateDir = dateFormat.format(new Date());
        String dateDir = "training/test";
        inputFilePaths = CommonStaticClass.getNewFiles("hdfs://namenode:9000/input/" + dateDir);
        CommonStaticClass.createWordDict(dictFileName);
        CommonStaticClass.createStopWordDict();
        Configuration conf = HBaseConfiguration.create();
        System.out.print("Would you like create the word-frequency ?(just the first run need):");
        //String input = in.nextLine();
        String input = "n";
        if (input.equals("y")) {// if input="y" then create the word frequency
            WordSegmentationMain.createWordFrequencyJob(conf);
        }
        String outPath = "hdfs://namenode:9000/output/" + dateDir;
        //segmentation for all job files and output to hdfs(output)
//        WordSegmentationMain.createWordSegmentationJob(conf, outPath);

        calDocLength(conf, outPath);//calculate the file length
//        calDocFrequency(conf,outPath);//calculate the file's doc frequency
//        calTermFrequency(conf,outPath);//calculate the file's term frequency
//        readAllDatasFromHBase(conf);

        //read the segmentated output result and write to independent file by line.
        //CommonStaticClass.readHDFSToFiles(dateDir);
        String[] queries = {"自动化", "研究所"};
        NavigableMap<String, String> res = queryRanking(conf, dateDir, queries);
        System.out.println(res);
    }

}
