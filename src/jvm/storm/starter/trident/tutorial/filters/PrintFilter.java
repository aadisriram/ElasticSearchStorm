package storm.starter.trident.tutorial.filters;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Print Filter for printing Trident tuples;
 * useful for testing and debugging.
 *
 * @author Aaditya Sriram
*/
public class PrintFilter extends BaseFilter {
	@Override
	public boolean isKeep(TridentTuple tuple) {
		System.err.println(tuple );
		return true;
	}
}
