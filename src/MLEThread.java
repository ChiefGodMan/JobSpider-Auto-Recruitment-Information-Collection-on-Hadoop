import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.coprocessor.TestCoprocessorConfiguration;

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;
import java.util.ArrayList;

/**
 * Created by ailias on 12/31/16.
 */


public class MLEThread extends Thread {

    private String threadName;
    private int startIndex;
    private int stopIndex;
    private TreeMap<Double,Integer> probMap = new TreeMap<>();
    ArrayList<List<String>> segs;
    HTable wordTable ;

    public MLEThread(String name, int start, int stop, ArrayList segs,HTable wordTable) {
        this.threadName = name;
        this.startIndex = start;
        this.stopIndex = stop;
        this.segs = segs;
        this.wordTable = wordTable;
    }

    public void run() {
        for (int i = startIndex; i < stopIndex; i++) {
            try {
                double prob = NGram.getMLE(segs.get(i),wordTable);
                probMap.put(prob,i);
            } catch (IOException e) {
                System.out.println(e);
            }
        }
    }

    /**
     * return the calculated probabilities values map
     * @return
     */
    public TreeMap<Double,Integer> getProbMap() {
        return this.probMap;
    }
}
