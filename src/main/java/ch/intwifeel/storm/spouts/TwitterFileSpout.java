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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A spout that uses Twitter streaming API for continuously
 * getting tweets
 */
public class TwitterFileSpout extends BaseRichSpout
{
    // Twitter API authentication credentials
    String custkey, custsecret;
    String accesstoken, accesssecret;

    // To output tuples from spout to the next stage bolt
    SpoutOutputCollector collector;

    // To read tweets from file instead of Twitter
    TweetFromFile tweetFromFile;


    private MongoClient mongoClient;

    private Set<String> productList = new HashSet<>();

   private class TweetFromFile {
       BufferedReader reader = null;


       public TweetFromFile(){
           File file = new File("tweetfile.txt");
           try {
               reader = new BufferedReader(new FileReader(file));
           }catch (IOException e) {
               e.printStackTrace();
           }

       }
       public String nextText(){
           try {
               int state=1;
               String line=null;
               while (state !=4 &&(line = reader.readLine()) != null  ) {
                   if (state==1){
                       if (line.startsWith("-user")){
                           state=2;
                       }}
                   else if (state==2){
                       state=3;
                   }
                   else if (state==3) {
                       //           System.out.println(line);
                       state=4;
                   }
               }

               return line;
           } catch (IOException e) {
               e.printStackTrace();
           }
           return null;
       }

       public void close(){
           try {
               reader.close();
           } catch (IOException e) {
               e.printStackTrace();
           }
       }


   }

    /**
     * Constructor for tweet spout that accepts the credentials
     */
    public TwitterFileSpout(
            String key,
            String secret,
            String token,
            String tokensecret)
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
        MongoDatabase db = mongoClient.getDatabase("intwifeel");

        MongoCollection<Document> products = db.getCollection("product");
        for (Document document : products.find()) {
            if (!productList.contains(document.get("name").toString())) {
                //TODO call Twitter here for the word
                productList.add(document.get("name").toString());
            }
        }


        // save the output collector for emitting tuples
        collector = spoutOutputCollector;

        //reader from file
        tweetFromFile= new TweetFromFile();

    }

    @Override
    public void nextTuple()
    {
        // try to pick a tweet from the file reader
        String ret = tweetFromFile.nextText();

        // if no tweet is available, wait for 50 ms and return
        if (ret==null)
        {
            Utils.sleep(50);
            return;
        }

        // now emit the tweet to next stage bolt
        //TODO have to connect to Mongo to get new words/products/terms, start stream afterwards

        collector.emit(new Values(ret, "Term2"));
    }

    @Override
    public void close()
    {
        // shutdown the stream - when we are going to exit
        //twitterStream.shutdown();
        tweetFromFile.close();

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
