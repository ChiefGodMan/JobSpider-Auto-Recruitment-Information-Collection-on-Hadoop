import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.*;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Created by ailias on 7/14/16.
 */

public class WordSegmentationMain {
    public final static int wordLen = 4;//specify the max length of one word
    public final static int sentenceLen = 20; //specify the max length of one sentence
    public static long wordCount;
    public final static String dilimt = " 、，：；。？！“”‘’《》（）【】￥—— .,;?/\\<>()[]{}!~`@#$%^&*_+-=:";
    public final static String tableName = "WordFrequency", columnFamily = "wordfrequency";
    public final static String dictFileName = "files/Dict.txt";
    public final static String stopDictFileName = "files/ch_stopword.txt";
    public final static String queryFileName = "files/query.txt";

    public static long docNum = 0;
    public static TreeMap<String, Double> docLengthMap = new TreeMap<String, Double>();//store <docpath,length>
    public static TreeMap<String, Double> docFrequencyMap = new TreeMap<String, Double>();//store <item,docfrequency>
    public static long avgDocLen = 0;
    public static long totDocLen = 0;//tottal docs lenght to compute the avgdoclen
    public final static double k1 = 1.5;
    public final static double b = 0.75;


    //*************************begin of word segmentation**************************************//

    /**
     * 对于给定的训练预料统计其中的词语的次数用于计算其频率，该函数用于调用mapreduce任务
     * Just statistic the word frequency no matter what it belongs to which doc.
     * used to segmentation for sentence.
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void createWordFrequencyJob(Configuration conf, String inputFilePath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "WordFrequency");
        job.setJarByClass(WordSegmentationMain.class);
        job.setMapperClass(WordFrequencyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        TableMapReduceUtil.initTableReducerJob(WordSegmentationMain.tableName, WordFrequencyReduder.class, job);
        FileInputFormat.addInputPath(job, new Path(inputFilePath));
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
    public static void createWordSegmentationJob(Configuration conf, String inputFilePath, String outPath) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, "WordSegmentation");
        job.setJarByClass(WordSegmentationMain.class);
        job.setMapperClass(WordSegmentationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(WordSegmentationReduder.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputFilePath));

        FileSystem fs = DistributedFileSystem.get(URI.create(outPath), conf);
        if (fs.exists(new Path(outPath))) {
            fs.delete(new Path(outPath), true);
        }

        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.waitForCompletion(true);
    }

    //************************************end of word segmentation********************************************************//


    //*******************************************begin of inverse indexing job*******************************************//

    /**
     * calculate the doc length in each file to hbase for BM25 calculation
     * output the <docName,wordLen> pairs to hbase
     *
     * @param conf
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void calDocLength(Configuration conf, String inputPath) throws IOException, InterruptedException, ClassNotFoundException {
        //1.this block code is used to count the document length
        String tableName = "DocLength";
        Job docLenjob = Job.getInstance(conf, "DocLength");
        //docLenjob.addFileToClassPath(new Path("hdfs://namenode:9000/hbase/lib/*.jar"));
        docLenjob.setJarByClass(WordSegmentationMain.class);
        docLenjob.setMapperClass(DocLengthMapper.class);
        docLenjob.setMapOutputKeyClass(Text.class);
        docLenjob.setMapOutputValueClass(Text.class);
        TableMapReduceUtil.initTableReducerJob(tableName, DocLengthReducerHBase.class, docLenjob);
        docLenjob.setNumReduceTasks(5);//reducer task's number
        FileInputFormat.addInputPath(docLenjob, new Path(inputPath));
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
    public static void calDocFrequency(Configuration conf, String inputPath) throws IOException, InterruptedException, ClassNotFoundException {
        //2.this block code is used to count how many docs include this item
        String tableName = "DocFrequency";
        Job docFreqJob = Job.getInstance(conf, "DocFrequency");
        //docFreqJob.addFileToClassPath(new Path("hdfs://namenode:9000/hbase/lib/*.jar"));
        docFreqJob.setJarByClass(WordSegmentationMain.class);
        docFreqJob.setMapperClass(DocFrequencyMapper.class);
        docFreqJob.setMapOutputKeyClass(Text.class);
        docFreqJob.setMapOutputValueClass(Text.class);
        TableMapReduceUtil.initTableReducerJob(tableName, DocFrequencyReducerHBase.class, docFreqJob);
        docFreqJob.setNumReduceTasks(2);//reducer task's number
        FileInputFormat.addInputPath(docFreqJob, new Path(inputPath));
        docFreqJob.waitForCompletion(true);
    }


    /**
     * calculate the term frequency for all files and store the (item+path,times) to the hbase
     * used for inverse indexing, which is different from WordFrequency.
     *
     * @param conf
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void calTermFrequency(Configuration conf, String inputPath) throws IOException, InterruptedException, ClassNotFoundException {
        //3.this block code is used to count each item frequency in each document
        String tableName = "TermFrequency";
        Job termFreqJob = Job.getInstance(conf, "TermFrequency");
        //termFreqJob.addFileToClassPath(new Path("hdfs://namenode:9000/hbase/lib/*.jar"));
        termFreqJob.setJarByClass(WordSegmentationMain.class);
        termFreqJob.setMapperClass(TermFrequencyMapper.class);
        termFreqJob.setMapOutputKeyClass(Text.class);
        termFreqJob.setMapOutputValueClass(Text.class);
        TableMapReduceUtil.initTableReducerJob(tableName, TermFrequencyReducerHBase.class, termFreqJob);
        termFreqJob.setNumReduceTasks(2);//reducer task's numbe
        FileInputFormat.addInputPath(termFreqJob, new Path(inputPath));
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
    public static NavigableMap<Double, String> queryRanking(Configuration conf, String DateDir, String queries[]) throws Throwable, IOException {
        //final sorting code
        String tableName = "TermFrequency";
        HTable queryTable = new HTable(conf, tableName);
        Scan queryScan = new Scan();
        ResultScanner resultScanner;
        HashMap<String, Double> pathTimesHM[] = new HashMap[queries.length];//used to store the scan result
        for (int i = 0; i < queries.length; i++) {//init map variable
            pathTimesHM[i] = new HashMap();
        }
        for (int i = 0; i < queries.length; i++) {//scan the hbase for each query parameter
            queryScan.setRowPrefixFilter((queries[i]).getBytes());
            resultScanner = queryTable.getScanner(queryScan);
            for (Result res : resultScanner) {//each query result
                for (KeyValue kv : res.list()) {//store the path and timesaddrs to pathTimesHM for the BM25 calculation
                    String itemPathStr = new String(kv.getRow());
                    String itempathPair[] = itemPathStr.split("&");
                    //add it to pathTimesHM.
                    double times;
                    if (itempathPair[1].compareTo(DateDir) > 1) {//it means the path is specified date content url.
                        if (pathTimesHM[i].containsKey(itempathPair[1])) {
                            times = pathTimesHM[i].get(itempathPair[1]) + Double.parseDouble(new String(kv.getValue()));
                        } else {
                            times = Double.parseDouble(new String(kv.getValue()));
                        }
                        pathTimesHM[i].put(itempathPair[1], times);
                    }
                }
            }
        }
        //next calculate the bm25 ranking values
        double doclen = 0;//document length
        double docfre = 0;//item's document frequency
        double tf = 0;
        double totbm25 = 0;
        double tmpbm25 = 0;
        TreeMap<String, Double> BM25 = new TreeMap<>();
        for (int i = 0; i < queries.length; i++) {//each query
            if (docFrequencyMap.containsKey(queries[i])) {//only there exist the query key ,then we can calculate the BM25 value.
                for (String path : pathTimesHM[i].keySet()) {//get each query's path
                    doclen = docLengthMap.get(path);//get doc length
                    docfre = docFrequencyMap.get(queries[i]);//get doc frequency for one item
                    tf = pathTimesHM[i].get(path) / doclen;//calculate the term frequency
                    tmpbm25 = Math.log((double) docNum / docfre) * (k1 + 1) * tf / (k1 * ((1 - b) + b * doclen / avgDocLen) + tf);
                    if (BM25.containsKey(path)) {//sum the rank for all queries
                        totbm25 = BM25.get(path) + tmpbm25;
                    } else
                        totbm25 = tmpbm25;
                    BM25.put(path, totbm25);
                }

            } else {
                System.out.format("Sorry, your input query string '%s' can not be found!\n", queries[i]);
            }
        }//end
        TreeMap<Double, String> result = new TreeMap<>();
        for (String path : BM25.keySet()) {
            result.put(BM25.get(path), path);
        }
        return result.descendingMap();
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
                tmpdocFrequencyMap.put(new String(kv.getRow()), Double.parseDouble(new String(kv.getValue())));
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
                double docl = Double.parseDouble(new String(kv.getValue()));
                docLengthMap.put(new String(kv.getRow()), docl);
                totDocLen += docl;
            }
            docNum += 1;
        }
        if (docNum > 0)
            avgDocLen = totDocLen / docNum;
        docLengthScanner.close();
        docLengthTable.close();
    }

    /**
     * create all we needed hbase tables to the system.
     *
     * @param conf
     * @throws IOException
     */
    public static void createHBaseTables(Configuration conf) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor tableDescriptor;
        String[] tablesName = {"WordFrequency", "DocFrequency", "DocLength", "TermFrequency"};
        for (String name : tablesName) {
            if (!admin.tableExists(name)) {//create only when there is no this table.
                tableDescriptor = new HTableDescriptor(TableName.valueOf(name));
                tableDescriptor.addFamily(new HColumnDescriptor(name.toLowerCase()));
                admin.createTable(tableDescriptor);
            }
        }
        admin.close();
    }

    /**
     * read all datas from hbase for BM25
     * process the init case
     *
     * @param conf
     * @throws IOException
     */
    public static void readAllDatasFromHBase(Configuration conf) throws IOException, Throwable {
        docFrequencyMap = readAllDocFrequency(conf);
        readAllDocLength(conf);
    }

    /**
     * upload local files to hdfs
     *
     * @param conf
     * @param src
     * @param dst
     * @throws IOException
     */
    public static void uploadFile(Configuration conf, String src, String dst) throws IOException {
        System.out.println("Start copying local files to hdfs");
        FileSystem fs = FileSystem.get(URI.create("hdfs://namenode:9000/"), conf);
        Path srcPath = new Path(src); //原路径
        Path dstPath = new Path(dst); //目标路径
        //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
        fs.copyFromLocalFile(false, srcPath, dstPath);
        fs.close();
        System.out.println("Finished copying files.");
    }

    //******************************end of inverse indexing**********************************//

    public static void main(String[] args) throws Throwable {
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS", "hdfs://namenode:9000");
        //@@@Important: add the jar file to all node.
        //conf.set("mapred.jar", "WordSegmentation.jar");
        FileSystem fileSystem = FileSystem.get(conf);

        while (true) {
            DateFormat dirDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String dateDir = dirDateFormat.format(new Date());
            //String localFilePath = "NewsmthScrapy/NewsmthScrapy/files/" + dateDir;
            String inputFilePath = "hdfs://namenode:9000/input/" + dateDir;
            String outPath = "hdfs://namenode:9000/output/" + dateDir;
            if (fileSystem.exists(new Path(inputFilePath)) && !fileSystem.exists(new Path(outPath))) {
                System.out.println("Starting processing the files");
                //Load the word from dict
                CommonStaticClass.loadWordDict(fileSystem, dictFileName);
                //Load the stopWord from dict
                CommonStaticClass.loadStopWordDict(fileSystem, stopDictFileName);
                //create all we needed tables.
                createHBaseTables(conf);
                //copy local files to hdfs
                //uploadFile(conf, localFilePath, inputFilePath);
                //--------------------begin comment from here--------------------------------------//
                //read all docfrequency and doclength to map for queryRanking
                //mapreduce job for updating word frequency in the table.`
                WordSegmentationMain.createWordFrequencyJob(conf, inputFilePath);
                //mapreduce job for segmentation in all files and output to hdfs(output)
                WordSegmentationMain.createWordSegmentationJob(conf, inputFilePath, outPath);
                //mapreduce job for doc length about each input files.
                calDocLength(conf, outPath);//calculate the file length
                //mapreduce job for doc frequency that how many doc include the word.
                calDocFrequency(conf, outPath);//calculate the file's doc frequency
                //mapreduce job for term frequency that how many times the word appear in one doc.
                calTermFrequency(conf, outPath);//calculate the file's term frequency
                //---------------------End comment from here--------------------------------------//
                //tips: do not delete the next function call
                readAllDatasFromHBase(conf);//update the <item, value> pairs.
                ResultEmail sendEmail = new ResultEmail(fileSystem, queryFileName);
                HashMap<String, String[]> emailQueriesMap = sendEmail.getEmailQueriesMap();
                String[] resultArr = null;//get the result url array
                for (Map.Entry<String, String[]> queryEntry : emailQueriesMap.entrySet()) {
                    //Query string array
                    //String[] queries = {"人工智能", "深度学习", "神经网络", "机器学习", "图像识别"};
                    //inverse indexing for doc
                    NavigableMap<Double, String> res = queryRanking(conf, dateDir, queryEntry.getValue());
                    resultArr = new String[res.size()];
                    int index = 0;
                    for (Map.Entry<Double, String> resEntry : res.entrySet()) {
                        resultArr[index++] = resEntry.getValue().replace("-r-00000", "").replace("+", "/").replace("-", ":").split("=")[1];
                    }
                    sendEmail.sendEmail(fileSystem, dateDir, queryEntry.getKey(), resultArr);
                }
                Thread.sleep(1000 * 60);
            } else {
                System.out.println("Current time do not need process.");
                Thread.sleep(1000 * 60 );
            }
        }

    }


}
