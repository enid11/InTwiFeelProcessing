package ch.intwifeel.storm.bolts;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Date;

import static com.mongodb.client.model.Filters.eq;

public class TestClass {

    public static void main(String args[]) {

        //TODO If credentials needed, uncomment this and add credential as second parameter to MongoClient constructor
        //MongoCredential credential = MongoCredential.createCredential("user1", "intwifeel", "password1".toCharArray());

        MongoClient mongo = new MongoClient(new ServerAddress("localhost", 27017));

        MongoDatabase db = mongo.getDatabase("intwifeel");

        MongoCollection<Document> products = db.getCollection("product");
        MongoCollection<Document> scores = db.getCollection("score");

        Document myDoc = products.find(eq("name", "Term1Update")).first();
        System.out.println(myDoc);

        Document product = products.find(eq("name", "Term1Update")).first();

        Document score = new Document("score", 28)
                .append("date", new Date())
                .append("_class", "intwifeel.model.ScoreEntity")
                .append("product", new Document("$ref","product").append("$id", product.get("_id")));

        scores.insertOne(score);

        products.updateOne(product, new Document("$addToSet", new Document("scores", new Document("$ref","score").append(
                "$id", score.get("_id")))));
//
    }
}
