package ch.intwifeel.storm.bolts;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static com.mongodb.client.model.Filters.eq;

/**
 * A bolt that prints the word and count to redis
 */
public class ReportBolt extends BaseRichBolt
{
    private OutputCollector _collector;


    final static Logger LOG = LoggerFactory.getLogger(ReportBolt.class);
    // place holder to keep the connection to redis
    transient RedisConnection<String,String> redis;

    private MongoClient mongo;

    /** The queue holding tuples in a batch. */
    protected LinkedBlockingQueue<Tuple> queue = new LinkedBlockingQueue<Tuple>();

    /** The threshold after which the batch should be flushed out. */
    int batchSize = 100;

    /**
     * The batch interval in sec. Minimum time between flushes if the batch sizes
     * are not met. This should typically be equal to
     * topology.tick.tuple.freq.secs and half of topology.message.timeout.secs
     */

    int batchIntervalInSec = 45;
    /** The last batch process time seconds. Used for tracking purpose */

    long lastBatchProcessTimeSeconds = 0;



    @Override
    public void prepare(
            Map                     map,
            TopologyContext         topologyContext,
            OutputCollector         outputCollector)
    {
        // instantiate a redis connection
        RedisClient client = new RedisClient("localhost",6379);
        _collector = outputCollector;
        // initiate the actual connection
        redis = client.connect();

        //connect to Mongo
        //TODO If credentials needed, uncomment this and add credential as second parameter to MongoClient constructor
        //MongoCredential credential = MongoCredential.createCredential("user1", "intwifeel", "password1".toCharArray());

        mongo = new MongoClient(new ServerAddress("localhost", 27017));
    }



    @Override
    public void execute(Tuple tuple)
    {

        // access the column 'word'
        String word = tuple.getStringByField("word");

        String score = tuple.getStringByField("score");

        String exampleSentence = tuple.getStringByField("sentence");
        if(word.equalsIgnoreCase("")) {
            // publish the example sentence to redis using word as the key
            redis.set(word, exampleSentence);
            redis.expire(word, 20);
            if (isTickTuple(tuple)) {

                // If so, it is indication for batch flush. But don't flush if previous
                // flush was done very recently (either due to batch size threshold was
                // crossed or because of another tick tuple
                //
                if ((System.currentTimeMillis() / 1000 - lastBatchProcessTimeSeconds) >= batchIntervalInSec) {
                    LOG.debug("Current queue size is " + this.queue.size()
                            + ". But received tick tuple so executing the batch");
                    saveScoreToMongo();
                } else {
                    LOG.debug("Current queue size is " + this.queue.size()
                            + ". Received tick tuple but last batch was executed "
                            + (System.currentTimeMillis() / 1000 - lastBatchProcessTimeSeconds)
                            + " seconds back that is less than " + batchIntervalInSec
                            + " so ignoring the tick tuple");
                }
            } else {

                // Add the tuple to queue. But don't ack it yet.
                this.queue.add(tuple);
                int queueSize = this.queue.size();

                LOG.debug("current queue size is " + queueSize);
                if (queueSize >= batchSize) {
                    LOG.debug("Current queue size is >= " + batchSize
                            + " executing the batch");
                    saveScoreToMongo();
                }
            }
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // configure how often a tick tuple will be sent to our bolt
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 45);
        LOG.debug("timeout default " + Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS);
        return conf;
    }

    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    private void saveScoreToMongo() {

        MongoDatabase db = mongo.getDatabase("intwifeel");

        MongoCollection<Document> products = db.getCollection("product");
        MongoCollection<Document> scores = db.getCollection("score");

        LOG.debug("Finishing batch of size " + queue.size());
        lastBatchProcessTimeSeconds = System.currentTimeMillis() / 1000;
        List<Tuple> tuples = new ArrayList<Tuple>();
        queue.drainTo(tuples);
        List<Document> scoreList = new ArrayList<Document>();
        List<Document> productList = new ArrayList<Document>();
        for (Tuple tuple : tuples) {
            Document productDocument = products.find(eq("name", tuple.getStringByField("word"))).first();

            Document scoreDocument = new Document("score", Double.valueOf(tuple.getStringByField("score")))
                    .append("date", new Date())
                    .append("_class", "intwifeel.model.ScoreEntity")
                    .append("product", new Document("$ref","product").append("$id", productDocument.get("_id")));
            productDocument =  new Document("$addToSet", new Document("scores", new Document("$ref", "score").append(
                    "$id", scoreDocument.get("_id"))));
            productList.add(productDocument);
            scoreList.add(scoreDocument);
        }
        try {
            // Execute bulk insert


            scores.insertMany(scoreList);

            products.insertMany(productList);
            for (Tuple tuple : tuples) {
                this._collector.ack(tuple);
            }
        } catch (Exception e) {
            LOG.error("Unable to process " + tuples.size() + " tuples", e);
            // Fail entire batch
            for (Tuple tuple : tuples) {
                this._collector.fail(tuple);
            }
        }




    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // nothing to add - since it is the final bolt
    }
}
