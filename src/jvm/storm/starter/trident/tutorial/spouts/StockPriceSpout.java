//package com.algorithms.trident.spouts;
package storm.starter.trident.tutorial.spouts;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Random;

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;


import org.apache.commons.io.IOUtils;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * A Spout that emits fake tweets. It calculates a random probability distribution for hashtags and actor activity. It
 * uses a dataset of 500 english sentences. It has a fixed set of actors and subjects which you can also modify at your own will.
 * Tweet text is one of the random 500 sentences followed by a hashtag of one subject.
 * 
 * @author pere
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class StockPriceSpout implements IBatchSpout {

	private int batchSize;

        // IMPORTANT: the file should be in  data subdirectory of the pom.xml directory 
        private String DATA_PATH;	
	public final static String[] ACTORS = { "stefan", "dave", "pere", "nathan", "doug", "ted", "mary",
	    "rose" };
	public final static String[] LOCATIONS = { "Spain", "USA", "Spain", "USA", "USA", "USA", "UK",
	    "France" };
	public final static String[] SUBJECTS = { "berlin", "justinbieber", "hadoop", "life", "bigdata" };

	private double[] activityDistribution;
	private double[][] subjectInterestDistribution;
	private Random randomGenerator;
	private String[] sentences;

	private long tweetId = 0;

	public StockPriceSpout() throws IOException {
		this.batchSize = 5;
		this.DATA_PATH ="data/500_sentences_en.txt";
	}

	public StockPriceSpout(int batchSize) throws IOException {
		this.batchSize = batchSize;
	}

       public StockPriceSpout(String path) throws IOException {
                this.DATA_PATH = path;
                this.batchSize = 5;
        }

       public StockPriceSpout(String path, int batchSize) throws IOException {
                this.DATA_PATH = path;
                this.batchSize = batchSize;
        }

	@SuppressWarnings("unchecked")
        @Override
	public void open(Map conf, TopologyContext context) {
		// init
		System.err.println("Open Spout instance");

		File file = new File(DATA_PATH);
        	InputStream in = null;

		this.randomGenerator = new Random();
		// read a resource with 500 sample english sentences
		try {
			in = new FileInputStream(file);
			sentences = (String[]) IOUtils.readLines(in).toArray(new String[0]);

//			sentences = (String[]) IOUtils.readLines(
//			    ClassLoader.getSystemClassLoader().getResourceAsStream("500_sentences_en.txt")).toArray(new String[0]);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
		// will define which actors are more proactive than the others
		this.activityDistribution = getProbabilityDistribution(ACTORS.length, randomGenerator);
		// will define what subjects each of the actors are most interested in
		this.subjectInterestDistribution = new double[ACTORS.length][];
		for(int i = 0; i < ACTORS.length; i++) {
			this.subjectInterestDistribution[i] = getProbabilityDistribution(SUBJECTS.length, randomGenerator);
		}
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		// emit batchSize fake tweets
		for(int i = 0; i < batchSize; i++) {
			collector.emit(getNextTweet());
		}
	}

	@Override
	public void ack(long batchId) {
		// nothing to do here
	}

	@Override
	public void close() {
		// nothing to do here
	}

	@Override
	public Map getComponentConfiguration() {
		// no particular configuration here
		return new Config();
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("time", "symbol", "price", "quantity");
		//return new Fields("id", "text", "actor", "location", "date");
	}

	// --- Helper methods --- //
	// SimpleDateFormat is not thread safe!
	private SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss aa");

	private Values getNextTweet() {
		int actorIndex = randomIndex(activityDistribution, randomGenerator);
		String author = ACTORS[actorIndex];
		String text = sentences[randomGenerator.nextInt(sentences.length)].trim();
		return new Values(DATE_FORMAT.format(System.currentTimeMillis()), text.split(",")[1], text.split(",")[2], text.split(",")[3]);
	}

	/**
	 * Code snippet: http://stackoverflow.com/questions/2171074/generating-a-probability-distribution Returns an array of
	 * size "n" with probabilities between 0 and 1 such that sum(array) = 1.
	 */
	private static double[] getProbabilityDistribution(int n, Random randomGenerator) {
		double a[] = new double[n];
		double s = 0.0d;
		for(int i = 0; i < n; i++) {
			a[i] = 1.0d - randomGenerator.nextDouble();
			a[i] = -1 * Math.log(a[i]);
			s += a[i];
		}
		for(int i = 0; i < n; i++) {
			a[i] /= s;
		}
		return a;
	}

	private static int randomIndex(double[] distribution, Random randomGenerator) {
		double rnd = randomGenerator.nextDouble();
		double accum = 0;
		int index = 0;
		for(; index < distribution.length && accum < rnd; index++, accum += distribution[index - 1])
			;
		return index - 1;
	}

	public static void main(String[] args) throws IOException, ParseException {
		StockPriceSpout spout = new StockPriceSpout();
		spout.open(null, null);
		for(int i = 0; i < 30; i++)
			System.out.println(spout.getNextTweet());
	}
}
