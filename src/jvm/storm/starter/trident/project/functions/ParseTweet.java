package storm.starter.trident.project.functions;

import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;
import twitter4j.json.DataObjectFactory;



// Local functions
import storm.starter.trident.project.functions.Content;
import storm.starter.trident.project.functions.ContentExtracter;

/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class ParseTweet extends BaseFunction {
    private static final Logger log = LoggerFactory.getLogger(ParseTweet.class);

    private ContentExtracter extracter;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        // if(extracter == null) 
        //     extracter = new ContentExtracter();
        
        // String rawTweetJson = (String)tuple.get(0);
        // Status parsed = parse(rawTweetJson);
        // User user = parsed.getUser();

        // for (Content content : extracter.extract(parsed)) {
        //     collector.emit(new Values(parsed.getText(), parsed.getId(), user));
        // }

        Status parsed = (Status)tuple.get(0);
        User user = parsed.getUser();
        String userScreenName = user.getScreenName();
        collector.emit(new Values(parsed.getText(), parsed.getId(), userScreenName));
    }

    private Status parse(String rawJson){
        try {
            Status parsed = TwitterObjectFactory.createStatus(rawJson);
            return parsed;
        } catch (Exception e) {
            e.printStackTrace();
            log.warn("Invalid tweet json -> " + rawJson, e);
            return null;
        }
    }
}
