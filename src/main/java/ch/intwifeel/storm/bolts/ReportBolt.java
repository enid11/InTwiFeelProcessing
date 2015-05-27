package ch.intwifeel.storm.bolts;

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

import java.util.Date;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;

/**
 * A bolt that prints the word and count to redis
 */
public class ReportBolt extends BaseRichBolt
{
    // place holder to keep the connection to redis
    transient RedisConnection<String,String> redis;

    private MongoClient mongo;

    @Override
    public void prepare(
            Map                     map,
            TopologyContext         topologyContext,
            OutputCollector         outputCollector)
    {
        // instantiate a redis connection
        RedisClient client = new RedisClient("localhost",6379);

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

        // publish the example sentence to redis using word as the key
        redis.set(word, exampleSentence);
        redis.expire(word, 20);

        saveScoreToMongo(word, score);
    }

    private void saveScoreToMongo(String word, String score) {
        MongoDatabase db = mongo.getDatabase("intwifeel");

        MongoCollection<Document> products = db.getCollection("product");
        MongoCollection<Document> scores = db.getCollection("score");

        Document product = products.find(eq("name", word)).first();

        Document scoreDocument = new Document("score", Double.valueOf(score))
                .append("date", new Date())
                .append("_class", "intwifeel.model.ScoreEntity")
                .append("product", new Document("$ref","product").append("$id", product.get("_id")));

        scores.insertOne(scoreDocument);

        products.updateOne(product, new Document("$addToSet", new Document("scores", new Document("$ref","score").append(
                "$id", scoreDocument.get("_id")))));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // nothing to add - since it is the final bolt
    }
}
