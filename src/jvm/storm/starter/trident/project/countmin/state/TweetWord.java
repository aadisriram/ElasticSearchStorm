package storm.starter.trident.project.countmin.state;

public class TweetWord {
    public String word;
    public long count; 

    @Override
    public boolean equals(Object obj) {
        return this.word.equals(((TweetWord)obj).word);    
    }
}