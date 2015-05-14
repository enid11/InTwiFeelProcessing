package ch.intwifeel.storm.bolts;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

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
        String id = tuple.getString(0);
        String sentence = tuple.getString(1);
        String score= Integer.toString(0);//sentiment analysis logic

        // emit the word and count
        collector.emit(new Values(id,sentence, score));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a two columns called 'word' and 'count'

        // declare the first column 'word', second column 'count'
        outputFieldsDeclarer.declare(new Fields("id","sentence","score"));
    }
}
