package storm.starter.trident.project.countmin; 

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
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.tuple.Values;


import storm.trident.operation.builtin.Count;

import storm.starter.trident.project.countmin.state.CountMinSketchStateFactory;
import storm.starter.trident.project.countmin.state.CountMinQuery;
import storm.starter.trident.project.countmin.state.CountMinSketchUpdater;
import storm.starter.trident.project.countmin.state.TopKQuery;
import storm.starter.trident.tutorial.functions.SplitFunction;
import storm.starter.trident.project.functions.BloomFilter;

import storm.starter.trident.project.functions.TweetBuilder;
import storm.starter.trident.project.functions.DocumentBuilder;
import storm.starter.trident.project.functions.ExtractDocumentInfo;
import storm.starter.trident.project.functions.ExtractSearchArgs;
import storm.starter.trident.project.functions.SentenceBuilder;
import storm.starter.trident.project.functions.CreateJson;
import storm.starter.trident.project.spouts.TwitterSampleSpout;
import storm.starter.trident.project.functions.ParseTweet;
import storm.starter.trident.project.filters.Print;
import storm.starter.trident.project.filters.PrintFilter;
import storm.starter.trident.project.functions.Tweet;

import storm.starter.trident.project.functions.ToLowerCase;

/**
 *@author: Preetham MS (pmahish@ncsu.edu)
 *Modified by Aaditya Sriram (asriram4@ncsu.edu)
 */


public class CountMinSketchTopology {

	 public static StormTopology buildTopology( LocalDRPC drpc ) {

        TridentTopology topology = new TridentTopology();

        int width = 100;
		int depth = 150;
		int seed = 100;

		String consumerKey = "v9BIUekmGglxjrty77VbNZdPM";
    	String consumerSecret = "qD4mnlXLHPdhbDOrZvGSRJjv0UEzwxZD3cVbrSqQbWgMXRDn83";
    	String accessToken = "25076520-1T97cLiNDmjqAgFcBDcnwPURkyxh1Ta6WQlTSZkpS";
    	String accessTokenSecret = "NWnJflgeXljmeKtzkbei3oigd1ung2lahm3vYxJje7M60";

        // Twitter topic of interest
        //String[] arguments = args.clone();
        String[] topicWords = {"love", "hate"};
        //String[] topicWords = {};
        // Create Twitter's spout
		TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey, consumerSecret,
									accessToken, accessTokenSecret, topicWords);

  //   	FixedBatchSpout spoutFixedBatch = new FixedBatchSpout(new Fields("sentence"), 3,
		// 	new Values("the cow jumped over the moon"),
		// 	new Values("the man went to the store and bought some candy"),
		// 	new Values("four score and seven years ago"),
		// 	new Values("how many apples can you eat"),
		// 	new Values("to be or not to be the person"))
		// 	;
		// spoutFixedBatch.setCycle(false);
            
		TridentState countMinDBMS = topology.newStream("tweets", spoutTweets)
			.each(new Fields("tweet"), new ParseTweet(), new Fields("text", "tweetId", "user"))
			.each(new Fields("text", "tweetId", "user"), new SentenceBuilder(), new Fields("sentence"))
			.each(new Fields("sentence"), new Split(), new Fields("wordsl"))
			.each(new Fields("wordsl"), new ToLowerCase(), new Fields("lWords"))
			.each(new Fields("lWords"), new BloomFilter(), new Fields("words"))
			.each(new Fields("words"), new FilterNull())
			.partitionPersist(new CountMinSketchStateFactory(depth,width,seed), new Fields("words"), new CountMinSketchUpdater())
			;


		topology.newDRPCStream("get_count", drpc)
			.each(new Fields("args"), new Split(), new Fields("query"))
			.stateQuery(countMinDBMS, new Fields("query"), new CountMinQuery(), new Fields("count"))
			.project(new Fields("query", "count"))
			;

		topology.newDRPCStream("get_topk", drpc)
			.stateQuery(countMinDBMS, new Fields("args"), new TopKQuery(), new Fields("topk"))
			.project(new Fields("topk"))
			;

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
        conf.setDebug( false );
        conf.setMaxSpoutPending( 10 );

        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        	
        //cluster.submitTopology("get_count",conf, buildTopology(drpc));
        cluster.submitTopology("get_count", conf, buildTopology(drpc));

        for (int i = 0; i < 100; i++) {
            //System.out.println("DRPC RESULT: " + drpc.execute("get_count","love hate"));
            System.out.println("DRPC RESULT TOPK: " + drpc.execute("get_topk",""));
            Thread.sleep(3000);
        }

		System.out.println("STATUS: OK");
		//cluster.shutdown();
        	//drpc.shutdown();
	}
}
