package storm.starter.trident.project;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.tuple.TridentTuple;
import storm.trident.testing.MemoryMapState;
import storm.trident.state.StateFactory;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.MatchQueryBuilder;

import static org.elasticsearch.node.NodeBuilder.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.node.Node;


import com.github.tlrx.elasticsearch.test.EsSetup;
import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.deleteIndex;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

// Local spouts, functions and filters
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

import backtype.storm.spout.ShellSpout;
import backtype.storm.topology.*;

import java.io.IOException;
import java.util.Arrays;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.github.fhuss.storm.elasticsearch.mapper.impl.DefaultTupleMapper;
import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.state.ESIndexMapState;
import com.github.fhuss.storm.elasticsearch.state.ESIndexUpdater;
import com.github.fhuss.storm.elasticsearch.state.ESIndexState;
import com.github.fhuss.storm.elasticsearch.state.QuerySearchIndexQuery;
import com.github.fhuss.storm.elasticsearch.ClientFactory.LocalTransport;
import com.github.fhuss.storm.elasticsearch.ClientFactory.*;

import org.elasticsearch.index.query.FilterBuilders.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
* This topology shows how to build ElasticSearch engine for a stream made of
* fake tweets and how to query it, using DRPC calls. 
* This example should be intended as
* an example of {@link TridentState} custom implementation.
* Modified by YOUR NAME from original author:
* @author Nagiza Samatova (samatova@csc.ncsu.edu)
* @author Preetham Srinath (pmahish@ncsu.edu)
*/
public class RealTimeElasticSearchTopology {

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
	
    public static StormTopology buildTopology(String[] args, Settings settings, LocalDRPC drpc)
            throws IOException {

		TridentTopology topology = new TridentTopology();

    	ESIndexMapState.Factory<Tweet> stateFactory = ESIndexMapState
	      .nonTransactional(new ClientFactory.NodeClient(settings.getAsMap()), Tweet.class);
        
		TridentState staticState = topology
              .newStaticState(new ESIndexState.Factory<Tweet>(
              new NodeClient(settings.getAsMap()), Tweet.class));
	
    	/**
		* Create spout(s)
		**/
		// Twitter's account credentials passed as args
        //String consumerKey = args[0];
        //String consumerSecret = args[1];
        //String accessToken = args[2];
        //String accessTokenSecret = args[3];

        // Twitter topic of interest
        //String[] arguments = args.clone();
        //String[] topicWords = Arrays.copyOfRange(arguments, 4, arguments.length);
        String consumerKey = "v9BIUekmGglxjrty77VbNZdPM";
    	String consumerSecret = "qD4mnlXLHPdhbDOrZvGSRJjv0UEzwxZD3cVbrSqQbWgMXRDn83";
    	String accessToken = "25076520-1T97cLiNDmjqAgFcBDcnwPURkyxh1Ta6WQlTSZkpS";
    	String accessTokenSecret = "NWnJflgeXljmeKtzkbei3oigd1ung2lahm3vYxJje7M60";

        // Twitter topic of interest
        //String[] arguments = args.clone();
        String[] topicWords = {"love", "hate"};
        // Create Twitter's spout
		TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey, consumerSecret,
									accessToken, accessTokenSecret, topicWords);
									
		//RandomSentenceSpout spoutTweets = new RandomSentenceSpout();
		/***
		* Here is the useful spout for debugging as it could be of finite size
		* Change setCycle(false) to setCycle(true) if 
		* continuous stream of tweets is needed
		**/
		// FixedBatchSpout spoutFixedBatch = new FixedBatchSpout(new Fields("sentence"), 3,
		// 	new Values("the cow jumped over the moon"),
		// 	new Values("the man went to the store and bought some candy"),
		// 	new Values("four score and seven years ago"),
		// 	new Values("how many apples can you eat"),
		// 	new Values("to be or not to be the person"))
		// 	;
		// 	spoutFixedBatch.setCycle(false);
		
		
        /**
         * Read a stream of tweets, parse and extract
         * only the text, and its id. Store the stream
         * using the {@link ElasticSearchState} implementation for Trident.
		 * NOTE: Commented lines are for debugging only
		 * TO DO FOR PROJECT 2: PART B:
		 *    1. Write a spout that ingests real text data stream:
		 * 		 You are allowed to utilize TweeterSampleSpout provided (DEAFULT), or
		 * 		 you may consider writing a spout that ingests Wikipedia updates
		 *		 or any other text stream (e.g., using Google APIs to bring a synopsis of a URL web-page
		 *		 for a stream of URL web-pages).
		 *	  2. Write proper support/utility functions to prepare your text data
		 * 		 for passing to the ES stateFactory: e.g., WikiUpdatesDocumentBuilder(), 
		 *		 GoogleWebURLSynopsisBuilder().
         */
		// topology.newStream("tweets", spoutFixedBatch)
		// 	//.each(new Fields("sentence"), new Print("sentence field"))
		// 	.parallelismHint(1)
		// 	.each(new Fields("sentence"), new DocumentBuilder(), new Fields("document"))
		// 	.each(new Fields("document"), new ExtractDocumentInfo(), new Fields("id", "index", "type"))
		// 	//.each(new Fields("id"), new Print("id field"))
		// 	.groupBy(new Fields("index", "type", "id"))
		// 	.persistentAggregate(stateFactory,  new Fields("document"), new TweetBuilder(), new Fields("tweet"))
		// 	//.each(new Fields("tweet"), new Print("tweet field"))
		// 	.parallelismHint(1) 
  //           ;
		
		/*** 
		* If you want to use TweeterSampleSpout instead of FixedBatchSpout
		* you will need to write a function SentenceBuilder()
		* to reuse the aforementioned code for populating ES index with tweets
		* using the code that looks like this:
		*/
		topology.newStream("tweets", spoutTweets)
            .each(new Fields("tweet"), new ParseTweet(), new Fields("text", "tweetId", "user"))
            // .each(new Fields("text","tweetId","user"), new PrintFilter("PARSED TWEETS:"))
            .each(new Fields("text", "tweetId", "user"), new SentenceBuilder(), new Fields("sentence"))
            .each(new Fields("sentence"), new DocumentBuilder(), new Fields("document"))
            .each(new Fields("document"), new ExtractDocumentInfo(), new Fields("id", "index", "type"))
			//.each(new Fields("id"), new Print("id field"))
			.groupBy(new Fields("index", "type", "id"))
			.persistentAggregate(stateFactory,  new Fields("document"), new TweetBuilder(), new Fields("tweet"))
			//.each(new Fields("tweet"), new Print("tweet field"))
            .parallelismHint(1) 
            ;
				
        /**
         * Now use a DRPC stream to query the state where the tweets are stored.
	 * CRITICAL: DO NOT CHANGE "query", "indicies", "types"
	 * WHY: QuerySearchIndexQuery() has hard-coded these tags in its code:
	 *     https://github.com/fhussonnois/storm-trident-elasticsearch/blob/13bd8203503a81754dc2a421accff216b665a11d/src/main/java/com/github/fhuss/storm/elasticsearch/state/QuerySearchIndexQuery.java 
         */
		 
	    topology
            .newDRPCStream("search_event", drpc)
            .each(new Fields("args"), new ExtractSearchArgs(), new Fields("query", "indices", "types"))
            .groupBy(new Fields("query", "indices", "types"))
            .stateQuery(staticState, new Fields("query", "indices", "types"), new QuerySearchIndexQuery(), new Fields("tweet"))
            .each(new Fields("tweet"), new FilterNull())
            .each(new Fields("tweet"), new CreateJson(), new Fields("json"))
            .project(new Fields("json"))
	        ; 


        return topology.build();
    }

    public static void main(String[] args) throws Exception {

		// Specify the name and the type of ES index 
		String index_name = new String();
		String index_type = new String();
		index_name = "my_index";
		index_type = "my_type";
	

		/**
		* Configure local cluster and local DRPC 
		***/
        Config conf = new Config();
		conf.setMaxSpoutPending(10);
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();

		/**
		* Configure ElasticSearch related information
		* Make sure that elasticsearch (ES) server is up and running
		* Check README.md file on how to install and run ES
		* Note that you can check if index exists and/or was created
		* with curl 'localhost:9200/_cat/indices?v'
		* and type: curl ‘http://127.0.0.1:9200/my_index/_mapping?pretty=1’
		* IMPORTANT: DO NOT CHANGE PORT TO 9200. IT MUST BE 9300 in code below.
		**/



		Settings settings = ImmutableSettings.settingsBuilder()
			.put("storm.elasticsearch.cluster.name", "elasticsearch")
			.put("storm.elasticsearch.hosts", "127.0.0.1:9300")
			.build();



		Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
    
		/** If you need more options, you can check test code:
		* https://github.com/tlrx/elasticsearch-test/blob/master/src/test/java/com/github/tlrx/elasticsearch/test/EsSetupTest.java
		* Remove else{} stmt if you do not want index to be deleted but rather updated
		* NOTE: Index files are stored by default under $HOME/elasticsearch-1.4.2/data/elasticsearch/nodes/0/indices
		**/
		EsSetup esSetup = new EsSetup(client);
		if (! esSetup.exists(index_name))
		{
			esSetup.execute(createIndex(index_name));
		}
		else
		{
			esSetup.execute(deleteIndex(index_name));
			esSetup.execute(createIndex(index_name));
		}

        cluster.submitTopology("state_drpc", conf, buildTopology(args, settings, drpc));

		System.out.println("STARTING DRPC QUERY PROCESSING");

	   /***
	   * TO DO FOR PROJECT 2: PART B:
	   *   3. Investigate Java APIs for ElasticSearch for various types of queries:
	   *		http://www.elasticsearch.com/guide/en/elasticsearch/client/java-api/current/index.html
	   *   4. Create at least 3 different query types besides termQuery() below
	   *		and test them with the DRPC execute().
	   *	    Example queries: matchQuery(), multiMatchQuery(), fuzzyQuery(), etc.
	   *   5. Can you thing of the type of indexing technologies ES might be using
	   *		to support efficient query processing for such query types: 
	   *		e.g., fuzzyQuery() vs. boolQuery().
	   ***/
       String query1 = QueryBuilders.commonTerms("text", "love").cutoffFrequency(0.001F).buildAsBytes().toUtf8();
	   String query2 = QueryBuilders.multiMatchQuery("love", "text").type(MatchQueryBuilder.Type.PHRASE_PREFIX).buildAsBytes().toUtf8();
	   String query3 = QueryBuilders.fuzzyQuery("text", "sock").buildAsBytes().toUtf8();
	   
       String drpcResult;
       for (int i = 0; i < 10; i++) {
          drpcResult = drpc.execute("search_event", query1+" "+index_name+" "+index_type);
          System.out.println("DRPC matchQuery: " + drpcResult);
		  drpcResult = drpc.execute("search_event", query2+" "+index_name+" "+index_type);
          System.out.println("DRPC multiMatchQuery: " + drpcResult);
	      drpcResult = drpc.execute("search_event", query3+" "+index_name+" "+index_type);
          System.out.println("DRPC fuzzyQuery: " + drpcResult);
          Thread.sleep(4000);
       }

        System.out.println("STATUS: OK");

        //cluster.shutdown();
        //drpc.shutdown();
		//esSetup.terminate(); 
    }
}
