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
}
