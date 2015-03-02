package storm.starter.trident.project.functions;

import backtype.storm.tuple.Values;
import com.google.common.base.Splitter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


/**
 * @author Aaditya Sriram (asriram4@ncsu.edu)
 */
public class NormalizeText extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        String string = tuple.getString(0);
        string = string.replaceAll("[^a-zA-Z0-9]", "");
        collector.emit(new Values(string.toLowerCase()));
    }
}
