package storm.starter.trident.project.functions;

import backtype.storm.tuple.Values;
import com.google.common.base.Splitter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import storm.starter.trident.project.countmin.state.MurmurHash;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author Aaditya Sriram (asriram4@ncsu.edu)
 */
public class TopKCalc extends BaseFunction {

    public static boolean[] murmurHashFilter = new boolean[1000];
    public static boolean[] javaHashFilter = new boolean[1000];

    static {
        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream("data/stop-word")));
            String line = input.readLine();
            while(line != null) {
                int murmurHash = Math.abs(MurmurHash.hash(line));
                int hash = Math.abs(line.hashCode());
                System.out.println(line + " : " + murmurHash%1000 + " : " + hash%1000);
                murmurHashFilter[murmurHash%1000] = true;
                javaHashFilter[hash%1000] = true;
                line = input.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        if(!isStopWord((String)tuple.get(0))) {
            collector.emit(new Values((String)tuple.get(0)));
        } else {
            collector.emit(new Values(new String()));
        }
    }

    private boolean isStopWord(String word) {
        int mHash = Math.abs(MurmurHash.hash(word));
        int sHash = Math.abs(word.hashCode());
        if(murmurHashFilter[mHash%1000] && javaHashFilter[sHash%1000])
            return true;
        
        return false;
    }
}