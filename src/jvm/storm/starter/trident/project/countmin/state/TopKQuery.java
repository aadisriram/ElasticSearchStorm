//import CountMinSketchState;
package storm.starter.trident.project.countmin.state;

import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import java.util.List;
import java.util.ArrayList;
import backtype.storm.tuple.Values;

import storm.starter.trident.project.countmin.state.TweetWord;

/**
 *@author: Aaditya Sriram (asriram4@ncsu.edu)
 *This query method is to get the top K items from 
 *the count min sketch.
 */

public class TopKQuery extends BaseQueryFunction<CountMinSketchState, String> {
    public List<String> batchRetrieve(CountMinSketchState state, List<TridentTuple> args) {
        List<String> ret = new ArrayList<String>();
        String result = new String();

        //Looping through the current top-k words
        for(TweetWord tword : state.queue) {
            result +=  " " + tword.word + " : " + tword.count + ",";
        }

        if(result.length() > 0) {
            result = result.substring(0, result.length() - 1);
        }

        ret.add(result);
        return ret;
    }

    public void execute(TridentTuple tuple, String topK, TridentCollector collector) {
        collector.emit(new Values(topK));
    }    
}
