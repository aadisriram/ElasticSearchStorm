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

import backtype.storm.spout.ShellSpout;
import backtype.storm.topology.*;
import java.util.Map;

/**
 *@author: Preetham MS (pmahish@ncsu.edu)
 *Modified by Aaditya Sriram (asriram4@ncsu.edu)
 */


public class CountMinSketchTopology {

	public static class RandomSentenceSpout extends ShellSpout implements IRichSpout {

        public RandomSentenceSpout() {
            super("node", "randomsentence.js");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
           	declarer.declare(new Fields("tweet"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

	public static StormTopology buildTopology( String[] filterWords, LocalDRPC drpc ) {

        TridentTopology topology = new TridentTopology();

        int width = 100;
		int depth = 150;
		int seed = 100;
		int topk_count = 5;

		String consumerKey = System.getenv("TWITTER_CONSUMER_KEY");
    	String consumerSecret = System.getenv("TWITTER_CONSUMER_SECRET");
    	String accessToken = System.getenv("TWITTER_ACCESS_TOKEN_KEY");
    	String accessTokenSecret = System.getenv("TWITTER_ACCESS_TOKEN_SECRET");

        // Create Twitter's spout
		TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey, consumerSecret,
									accessToken, accessTokenSecret, filterWords);

        // NodeJS Spout
		// RandomSentenceSpout spoutTweets = new RandomSentenceSpout();
            
		TridentState countMinDBMS = topology.newStream("tweets", spoutTweets)
			.each(new Fields("tweet"), new ParseTweet(), new Fields("text", "tweetId", "user"))
			.each(new Fields("text", "tweetId", "user"), new SentenceBuilder(), new Fields("sentence"))
			.each(new Fields("sentence"), new Split(), new Fields("wordsl"))
			.each(new Fields("wordsl"), new ToLowerCase(), new Fields("lWords"))
			.each(new Fields("lWords"), new BloomFilter(), new Fields("words"))
			.each(new Fields("words"), new FilterNull())
			.partitionPersist(new CountMinSketchStateFactory(depth, width, seed, topk_count), new Fields("words"), new CountMinSketchUpdater())
			//.parallelismHint(3)
			;

		//Call this to get the count of words passed in the query
		topology.newDRPCStream("get_count", drpc)
			.each(new Fields("args"), new Split(), new Fields("query"))
			.stateQuery(countMinDBMS, new Fields("query"), new CountMinQuery(), new Fields("count"))
			.project(new Fields("query", "count"))
			;

		//Call this to get the top-K words
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
        	
        String[] filterWords = args.clone();
        //cluster.submitTopology("get_count",conf, buildTopology(drpc));
        cluster.submitTopology("get_count", conf, buildTopology(filterWords, drpc));

        for (int i = 0; i < 100; i++) {
        	//Query type to get the count for certain words
            // System.out.println("DRPC RESULT: " + drpc.execute("get_count","love hate"));

            //Query type to get the top-k items
            System.out.println("DRPC RESULT TOPK: " + drpc.execute("get_topk",""));
            Thread.sleep(3000);
        }

		System.out.println("STATUS: OK");
		//cluster.shutdown();
        //drpc.shutdown();
	}
}
