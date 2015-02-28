package storm.starter.trident.tutorial;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import storm.trident.operation.builtin.Count;

import storm.starter.trident.tutorial.spouts.CSVBatchSpout;
import storm.starter.trident.tutorial.filters.PrintFilter;
import storm.starter.trident.tutorial.functions.SplitFunction;

public class DRPCStockQueryTopology {
	// Path relative to pom.xml file
	private static final String DATA_PATH = "data/stocks.csv.gz";
	/*************
	 * Goals: (1) Start a Trident topology that reads stock symbols
	 * and prices from a CSV data file. (2) Query the topology
	 * with DRPC every 5 seconds.
	 *************/
	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setDebug(true);
		conf.setMaxSpoutPending(20);
		// This topology can only be run as local
		// TO DO: Create and submit a DRPC local cluster topology
		// Your goes here
                LocalCluster cluster = new LocalCluster();
		LocalDRPC drpc = new LocalDRPC();
                cluster.submitTopology("stockmarket_drpc", conf, buildTopology(drpc));

		for (int i = 0; i < 10; i++) {
			// TO DO: Make DRPC call with "trade_events" handler and
			// arguments "AAPL, GE, INTC"
			// TO DO: Print DRPC results to stdout
			// Your code goes here
                        System.err.println("DRPC RESULT: " + drpc.execute("trade_events", "AAPL GE INTC")); 
			Thread.sleep(5000);
		}

		cluster.shutdown();
		drpc.shutdown();
	}

	public static StormTopology buildTopology(LocalDRPC drpc) {
		// TO DO: Create CSV spout, called spoutCSV,
		// from the CSV file in DATA_PATH
		// Emit fields "date", "symbol",
		// "price", "shares"
		// Your code goes here
		// TO DO: Create TridentState in-memory hash map,
		// called tradeVolumeDBMS
		// It should ingest the data stream from the spoutCSV spout
		// Group the tuples by "symbol" field
		// Perform persistent aggregation and store (key,value)
		// in in-process memory map
		// where key is "shares" and "value"
		// is aggregated field called "volume"
		// NOTE: groupBy() creates a "grouped stream"
		// which means that subsequent aggregators
		// will only affect Tuples within a group.
		// groupBy() must always be followed by an
		// NOTE: You can debug output of what spout emits with
		// .each( new Fields( "date", "symbol",
		// "price", "shares" ), new Debug() )
		// Your code goes here
		TridentTopology topology = new TridentTopology();

		Fields inputFields = new Fields("date", "symbol", "price", "shares");
		CSVBatchSpout spoutCSV = new CSVBatchSpout(DATA_PATH, inputFields);

                TridentState tradeVolumeDBMS = topology
						.newStream("spout", spoutCSV)
						.groupBy(new Fields("symbol"))
						.persistentAggregate(new MemoryMapState.Factory(), new Fields("shares"), new Sum(), new Fields("volume"));

		/**
		 * TO DO: Now setup a another stream--DRPC stream.
		 * The DRPC stream should generate a list of "symbols"
		 * passed as args by the client.
		 * It should query the persistent aggregate state,
		 * tradeVolumeDBMS, by "symbol" for
		 * the "volume" value.
		 * It should finally project() only
		 * the following fields of interest:
		 * "symbol" and "volume" that
		 * " DRPC client should print on stdout.
		 * NOTE: For debugging of args stmt use:
		 * .each( new Fields( "args" ), new Debug() )
		 * For debugging:
		 * .each( new Fields( "symbol" ), new Debug() )
		 * Debug print symbol and volume values:
		 * .each( new Fields( "symbol", "volume" ),
		 * new Debug() )
		 * To remove nulls for ’each’ value in the stream use
		 * .each( new Fields( "volume" ),
		 * new FilterNull() )
		 *****/
		
		// Your code goes here
		topology
			.newDRPCStream("trade_events", drpc)
			.each(new Fields("args"), new SplitFunction(" "), new Fields("symbols"))
			.groupBy(new Fields("symbols"))
			.stateQuery(tradeVolumeDBMS, new Fields("symbols"), new MapGet(), new Fields("volume"))
			.each(new Fields("volume"), new FilterNull())
			.project(new Fields("symbols", "volume"));
		
		
		return topology.build();
		// TO DO: Return the built topology
		// Your code goes here
	}
}
