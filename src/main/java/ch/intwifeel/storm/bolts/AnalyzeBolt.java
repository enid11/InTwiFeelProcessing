package ch.intwifeel.storm.bolts;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.util.Map;
import java.util.Properties;

/**
 * A bolt that counts the words that it receives
 */
public class AnalyzeBolt extends BaseRichBolt
{
    // To output tuples from this bolt to the next stage bolts, if any
    private OutputCollector collector;

    // Map to store the count of the words
    private Map<String, Integer> countMap;

    @Override
    public void prepare(
            Map                     map,
            TopologyContext         topologyContext,
            OutputCollector         outputCollector)
    {

        // save the collector for emitting tuples
        collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple)
    {
        // get the word from the 1st column of incoming tuple
        String sentence = tuple.getString(0);
        String word = tuple.getString(1);
        //String score= Integer.toString(0);//sentiment analysis logic
        String score = findSentiment(sentence);


        // emit the word and count
        collector.emit(new Values(sentence, score, word));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a two columns called 'word' and 'count'

        // declare the first column 'word', second column 'count'
        outputFieldsDeclarer.declare(new Fields("sentence","score", "word"));
    }

    public String findSentiment(String sentence) {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        int mainSentiment = 0;
        if (sentence != null && sentence.length() > 0) {
            int longest = 0;
            Annotation annotation = pipeline.process(sentence);

            for (CoreMap coreMap : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = coreMap.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);

                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = coreMap.toString();

                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }
            }
        }

        return Integer.toString(mainSentiment);
    }
}
