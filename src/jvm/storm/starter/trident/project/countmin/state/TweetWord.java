package storm.starter.trident.project.countmin.state;

import java.io.Serializable;

/*
* @author : Aaditya Sriram
* Each tweetword is an object that hold a word from
* the processed tweet, along with the current count
* estimate.
*/
public class TweetWord implements Serializable {

	//The actual word
    public String word;

    //Stored count estimate from the count min sketch
    public long count; 

    //Empty Default Constructor, just in case someone hates
    //using parameterized constructors
    public TweetWord() {

    }

    //Parameterized Constructor to initialize, duh
    public TweetWord(String word, long count) {
    	this.word = word;
    	this.count = count;
    }

    //Custom equals method for managing insertion and deletion from PQueue
    @Override
    public boolean equals(Object obj) {
        return this.word.equals(((TweetWord)obj).word);    
    }
}