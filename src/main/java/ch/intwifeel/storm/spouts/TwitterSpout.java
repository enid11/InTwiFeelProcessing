package ch.intwifeel.storm.spouts;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A spout that uses Twitter streaming API for continuously
 * getting tweets
 */
public class TwitterSpout extends BaseRichSpout
{
    // Twitter API authentication credentials
    String custkey, custsecret;
    String accesstoken, accesssecret;

    // To output tuples from spout to the next stage bolt
    SpoutOutputCollector collector;

    // Twitter4j - twitter stream to get tweets
    TwitterStream twitterStream;

    // Shared queue for getting buffering tweets received
    LinkedBlockingQueue<String> queue = null;

    private MongoClient mongoClient;
    private MongoDatabase db;
    long oldtime = 0;
    long difference = 60000;
    private Set<String> productList = new HashSet<>();

    // Class for listening on the tweet stream - for twitter4j
    private class TweetListener implements StatusListener {

        // Implement the callback function when a tweet arrives
        @Override
        public void onStatus(Status status)
        {
            // add the tweet into the queue buffer
            queue.offer(status.getText());
            if(oldtime == 0){
                oldtime = System.currentTimeMillis();
            }
            else if(System.currentTimeMillis()-oldtime >= difference){
                //change filters

                productList.clear();
                MongoCollection<Document> products = db.getCollection("product");
                for (Document document : products.find()) {
                        productList.add(document.get("name").toString());
                }
                twitterStream.filter(new FilterQuery().track(productList.toArray(new String[productList.size()])));

                //possibly check conditions for scaling
                oldtime =  System.currentTimeMillis();
            }


        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice sdn)
        {
        }

        @Override
        public void onTrackLimitationNotice(int i)
        {
        }

        @Override
        public void onScrubGeo(long l, long l1)
        {
        }

        @Override
        public void onStallWarning(StallWarning warning)
        {
        }

        @Override
        public void onException(Exception e)
        {
            e.printStackTrace();
        }
    };

    /**
     * Constructor for tweet spout that accepts the credentials
     */
    public TwitterSpout(
            String                key,
            String                secret,
            String                token,
            String                tokensecret)
    {
        custkey = key;
        custsecret = secret;
        accesstoken = token;
        accesssecret = tokensecret;
//        mongoClient = new MongoClient(new ServerAddress("localhost", 27017));
    }

    @Override
    public void open(
            Map                     map,
            TopologyContext         topologyContext,
            SpoutOutputCollector    spoutOutputCollector)
    {

        //Put this in a function that is called one
        mongoClient = new MongoClient(new ServerAddress("localhost", 27017));
        db = mongoClient.getDatabase("intwifeel");

        MongoCollection<Document> products = db.getCollection("product");
        for (Document document : products.find()) {
            if (!productList.contains(document.get("name").toString())) {
                //TODO call Twitter here for the word
                productList.add(document.get("name").toString());
            }
        }


        // create the buffer to block tweets
        queue = new LinkedBlockingQueue<String>(1000);

        // save the output collector for emitting tuples
        collector = spoutOutputCollector;


        // build the config with credentials for twitter 4j
        ConfigurationBuilder config =
                new ConfigurationBuilder()
                        .setOAuthConsumerKey(custkey)
                        .setOAuthConsumerSecret(custsecret)
                        .setOAuthAccessToken(accesstoken)
                        .setOAuthAccessTokenSecret(accesssecret);

        // create the twitter stream factory with the config
        TwitterStreamFactory fact =
                new TwitterStreamFactory(config.build());

        // get an instance of twitter stream
        twitterStream = fact.getInstance();

        // provide the handler for twitter stream
        twitterStream.addListener(new TweetListener());

        // start the sampling of tweet
        twitterStream.filter(new FilterQuery().track(productList.toArray(new String[productList.size()])));
        System.out.println("Terms to track"+ Arrays.toString(productList.toArray(new String[productList.size()])));
    }

    @Override
    public void nextTuple()
    {
        // try to pick a tweet from the buffer
        String ret = queue.poll();

        // if no tweet is available, wait for 50 ms and return
        if (ret==null)
        {
            Utils.sleep(50);
            return;
        }

        // now emit the tweet to next stage bolt
        //TODO have to connect to Mongo to get new words/products/terms, start stream afterwards
        System.out.println("tweet "+ ret);
        collector.emit(new Values(ret, "Term2"));
    }

    @Override
    public void close()
    {
        // shutdown the stream - when we are going to exit
        twitterStream.shutdown();
    }

    /**
     * Component specific configuration
     */
    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        // create the component config
        Config ret = new Config();

        // set the parallelism for this spout to be 1
        ret.setMaxTaskParallelism(1);

        return ret;
    }

    @Override
    public void declareOutputFields(
            OutputFieldsDeclarer outputFieldsDeclarer)
    {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a single column called 'tweet'
        outputFieldsDeclarer.declare(new Fields("tweet", "word"));
    }
}
