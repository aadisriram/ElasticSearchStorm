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
public class BloomFilter extends BaseFunction {

    public static boolean[] murmurHashFilter = new boolean[100000];
    public static boolean[] javaHashFilter = new boolean[100000];

    static {
        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream("data/stop-word")));
            String line = input.readLine();
            while(line != null) {
                int murmurHash = Math.abs(MurmurHash.hash(line));
                int hash = Math.abs(line.hashCode());
                murmurHashFilter[murmurHash%100000] = true;
                javaHashFilter[hash%100000] = true;
                line = input.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String str = ((String)tuple.get(0)).trim();
        if(!isStopWord(str)) {
            collector.emit(new Values(str));
        } else {
            collector.emit(new Values(new String()));
        }
    }

    private boolean isStopWord(String word) {

        if(word.length() < 3)
            return true;

        int mHash = Math.abs(MurmurHash.hash(word));
        int sHash = Math.abs(word.hashCode());
        if(murmurHashFilter[mHash%100000] && javaHashFilter[sHash%100000])
            return true;
        
        return false;
    }
}