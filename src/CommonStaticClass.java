import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.System.exit;

/**
 * Created by ailias on 1/16/17.
 */
public class CommonStaticClass {
    public static TreeSet<String> wordDict = new TreeSet<>();
    public static HashSet<String> stopWordsHS = new HashSet<String>();

    /**
     * create the word dict for segmentation processing
     *
     * @param fileName
     * @throws IOException
     */
    public static void loadWordDict(FileSystem fileSystem, String fileName) throws IOException {
        //System.out.println("Starting load the Dict!");
        Path filePath = new Path("hdfs://namenode:9000/" + fileName);
        //Path filePath = new Path(fileName);
        if (fileSystem.exists(filePath)) {
            FSDataInputStream inputStream = fileSystem.open(filePath);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String word;
            while ((word = bufferedReader.readLine()) != null) {
                wordDict.add(word);
            }
            if (wordDict.size() == 0) {
                System.err.println("Dict empty");
                exit(1);
            }
            bufferedReader.close();
        } else {
            System.out.println("Ailias@Can not find " + fileName);
            exit(-1);
        }
        //System.out.println("Finished load the Dict!");
    }

    /**
     * @param fileName
     */
    public static void loadStopWordDict(FileSystem fileSystem, String fileName) throws IOException {
        Path filePath = new Path("hdfs://namenode:9000/" + fileName);
        //Path filePath = new Path( fileName);
        if (fileSystem.exists(filePath)) {
            FSDataInputStream inputStream = fileSystem.open(filePath);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            //Chinese stop word read
            String stopWordsStr = null;
            while ((stopWordsStr = bufferedReader.readLine()) != null) {
                StringTokenizer stopTokens = new StringTokenizer(stopWordsStr);
                while (stopTokens.hasMoreTokens()) {
                    stopWordsHS.add(stopTokens.nextToken());
                }
            }
            if (stopWordsHS.size() == 0) {
                System.err.println("Stop Dict empty");
                exit(1);
            }
            bufferedReader.close();
        } else {
            System.out.println("Ailias@Can not find " + fileName);
            exit(-1);
        }
    }

//    /**
//     * Read the hdfs output file line by line and output to files for each line
//     * read the segmentation output file and to files by each line.
//     * @throws URISyntaxException
//     * @throws IOException
//     */
//    public static void readHDFSToFiles(String dateDir) throws URISyntaxException, IOException {
//        Configuration conf = new Configuration();
//        Path filepath = new Path("hdfs://namenode:9000/output/"+dateDir+"/part-r-00000");
//        FileSystem readHdfs = FileSystem.get(new URI("hdfs://namenode:9000"), conf);
//        BufferedReader inbr = new BufferedReader((new InputStreamReader(readHdfs.open(filepath))));
//
//        FileSystem writeHdfs = FileSystem.get(new URI("hdfs://namenode:9000"), conf);
//        writeHdfs.mkdirs(new Path("hdfs://namenode:9000/outfiles/" + dateDir));
//        String lineText;
//        while ((lineText = inbr.readLine()) != null) {//read each line and output to file alone
//            String fileName = lineText.split("http")[0].trim();
//            System.out.println(fileName);
//            Path outFilePath = new Path("hdfs://namenode:9000/outfiles/" + dateDir + "/" + fileName);
//            OutputStream outputStream = writeHdfs.create(outFilePath);
//            BufferedWriter outbw = new BufferedWriter(new OutputStreamWriter(outputStream));
//            outbw.write(lineText);
//            outbw.close();
//        }
//    }
//
//
//    /**
//     * find all hdfs files name for specified
//     *
//     * @param dirUri
//     * @throws URISyntaxException
//     * @throws IOException
//     */
//    public static List<String> getNewFiles(String dirUri) throws URISyntaxException, IOException {
//        Configuration conf = new Configuration();
//        FileSystem hdfs = FileSystem.get(new URI("hdfs://namenode:9000"), conf);
//        FileStatus fileStatus[] = hdfs.listStatus(new Path(dirUri));
//        List<String> inputFilePaths = new ArrayList<>();
//        Path paths[] = FileUtil.stat2Paths(fileStatus);
//        for (Path path : paths) {
//            inputFilePaths.add(path.toString());
//        }
//        return inputFilePaths;
//    }


}
