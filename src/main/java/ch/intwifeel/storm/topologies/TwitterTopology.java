package ch.intwifeel.storm.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import ch.intwifeel.storm.bolts.AnalyzeBolt;
import ch.intwifeel.storm.bolts.ReportBolt;
import ch.intwifeel.storm.spouts.TwitterSpout;

class TwitterTopology
{
    public static void main(String[] args) throws Exception
    {
        // create the topology
        TopologyBuilder builder = new TopologyBuilder();

        // now create the tweet spout with the credentials
        TwitterSpout tweetSpout = new TwitterSpout(
                "vbZg352rqYLxAgvdsISxNlnCA",
                "LqkeatfqJdVbO1dzDI27fd14TtLbnh8Z930vorLf6KQVE2M4QR",
                "3121240089-7BiKCJw7W6XZaUpSJIPAK3dvANruiNqnTHFMUmv",
                "wDOPXEwooZ4wSkds0hIaAUrO8nIHhY58z3SevAIJF7DAw"
        );

        // attach the tweet spout to the topology - parallelism of 1
        builder.setSpout("tweet-spout", tweetSpout, 1);

        // attach the analyze bolt using shuffle grouping - parallelism of 12
        builder.setBolt("analyze-bolt", new AnalyzeBolt(), 12).shuffleGrouping("tweet-spout");

        // attach the report bolt using global grouping - parallelism of 1
        builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("analyze-bolt");

        // create the default config object
        Config conf = new Config();

        // run it in a live cluster
        conf.setDebug(true);

        if (args != null && args.length > 0) {


            // set the number of workers for running all spout and bolt tasks
            conf.setNumWorkers(6);

            // create the topology and submit with config
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

        } else {
            // run it in a simulated local cluster

            // set the number of threads to run - similar to setting number of workers in live cluster
            conf.setMaxTaskParallelism(3);

            // create the local cluster instance
            LocalCluster cluster = new LocalCluster();

            // submit the topology to the local cluster
            cluster.submitTopology("tweet-analysis", conf, builder.createTopology());

            // let the topology run for 30 seconds. note topologies never terminate!
            Utils.sleep(30000);

            // now kill the topology
            cluster.killTopology("tweet-analysis");

            // we are done, so shutdown the local cluster
            cluster.shutdown();
        }
    }
}
