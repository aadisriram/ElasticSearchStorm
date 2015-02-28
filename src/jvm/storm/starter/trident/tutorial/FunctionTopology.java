package storm.starter.trident.tutorial;

import java.io.IOException;
import storm.trident.TridentTopology;
import storm.trident.Stream;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;


//Step 1
import storm.starter.trident.tutorial.spouts.FakeTweetsBatchSpout;
import storm.starter.trident.tutorial.filters.PrintFilter;
import storm.starter.trident.tutorial.filters.RegexFilter;
import storm.starter.trident.tutorial.functions.ToUpperCaseFunction;

/**
 * Skeleton Trident topology
 *
 * @author Aaditya Sriram
*/
public class FunctionTopology {

	//STEP 2
    private static final String DATA_PATH = "data/500_sentences_en.txt";

	public static StormTopology buildTopology() throws IOException {
		
		//Step 3
		Fields inputFields = new Fields("id", "text", "actor", "location", "date");

		//Step 4
    		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(DATA_PATH);

    		//Step 5
    		PrintFilter filter = new PrintFilter();

    		//Steps 6-7

    		//Step 8
    		TridentTopology topology = new TridentTopology();

    		//Step 9
    		Stream stream = topology.newStream("spout", spout);

    		//Step 10
    		stream
			.each(new Fields("actor"), new RegexFilter("doug"))
			.each(new Fields("actor", "text"), new ToUpperCaseFunction(), new Fields("uppercased_actor"))
			.each(new Fields("uppercased_actor"), new PrintFilter());

    		//Step 11
    		return topology.build();
	}

	public static void main(String[] args) throws Exception {

		Config conf = new Config();
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0],conf, buildTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("skeleton", conf, buildTopology());
		}
	}
}
