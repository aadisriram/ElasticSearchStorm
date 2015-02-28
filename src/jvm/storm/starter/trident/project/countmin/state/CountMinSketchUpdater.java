package storm.starter.trident.project.countmin.state;

import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import java.util.List;
import java.util.ArrayList;

/**
 *@author: Preetham MS (pmahish@ncsu.edu)
 */

public class CountMinSketchUpdater extends BaseStateUpdater<CountMinSketchState> {
    public void updateState(CountMinSketchState state, List<TridentTuple> tuples, TridentCollector collector) {
        List<Long> ids = new ArrayList<Long>();
        List<String> locations = new ArrayList<String>();
        for(TridentTuple t: tuples) {
            //ids.add(t.getLong(0));
            //locations.add(t.getString(1));

            state.add(t.getString(0),1);
        }
        
    }
}
