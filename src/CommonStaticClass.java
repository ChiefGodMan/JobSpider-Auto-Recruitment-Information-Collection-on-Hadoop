import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by ailias on 1/16/17.
 */
public class CommonStaticClass {
    public static TreeSet<String> wordDict = new TreeSet<>();
    public static HashSet<String> stopWordsHS = new HashSet<String>();

    /**
     * create the word dict for segmentation processing
     * @param path
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void createWordDict(String path) throws FileNotFoundException, IOException {
        System.out.println("Starting create the Dict!");
        BufferedReader file = new BufferedReader(new FileReader(path));
        String word;
        while ((word = file.readLine()) != null) {
            wordDict.add(word);
        }
        System.out.println("Finished create the Dict!");
    }

    public static void createStopWordDict(){
        BufferedReader br = null;
        try {
            //Chinese stop word read
            br = new BufferedReader(new FileReader("files/dict/ch_stopword.txt"));
            String stopWordsStr = null;
            while ((stopWordsStr = br.readLine()) != null) {
                StringTokenizer stopTokens = new StringTokenizer(stopWordsStr);
                while (stopTokens.hasMoreTokens()) {
                    stopWordsHS.add(stopTokens.nextToken());
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Read the hdfs output file line by line and output to files for each line
     * read the segmentation output file and to files by each line.
     * @throws URISyntaxException
     * @throws IOException
     */
    public static void readHDFSToFiles(String dateDir) throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        Path filepath = new Path("hdfs://namenode:9000/output/"+dateDir+"/part-r-00000");
        FileSystem readHdfs = FileSystem.get(new URI("hdfs://namenode:9000"), conf);
        BufferedReader inbr = new BufferedReader((new InputStreamReader(readHdfs.open(filepath))));

        FileSystem writeHdfs = FileSystem.get(new URI("hdfs://namenode:9000"), conf);
        writeHdfs.mkdirs(new Path("hdfs://namenode:9000/outfiles/" + dateDir));
        String lineText;
        while ((lineText = inbr.readLine()) != null) {//read each line and output to file alone
            String fileName = lineText.split("http")[0].trim();
            System.out.println(fileName);
            Path outFilePath = new Path("hdfs://namenode:9000/outfiles/" + dateDir + "/" + fileName);
            OutputStream outputStream = writeHdfs.create(outFilePath);
            BufferedWriter outbw = new BufferedWriter(new OutputStreamWriter(outputStream));
            outbw.write(lineText);
            outbw.close();
        }
    }


    /**
     * find all hdfs files name for specified
     *
     * @param dirUri
     * @throws URISyntaxException
     * @throws IOException
     */
    public static List<String> getNewFiles(String dirUri) throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(new URI("hdfs://namenode:9000"), conf);
        FileStatus fileStatus[] = hdfs.listStatus(new Path(dirUri));
        List<String> inputFilePaths = new ArrayList<>();
        Path paths[] = FileUtil.stat2Paths(fileStatus);
        for (Path path : paths) {
            inputFilePaths.add(path.toString());
        }
        return inputFilePaths;
    }


}
