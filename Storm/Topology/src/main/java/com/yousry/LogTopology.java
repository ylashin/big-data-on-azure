package com.yousry;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.eventhubs.spout.EventHubSpout;
import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import java.io.FileReader;
import java.util.Properties;


public class LogTopology {
    protected EventHubSpoutConfig spoutConfig;
    protected int numWorkers;

    protected void readEventHubConfig(String[] args) throws Exception {
        Properties properties = new Properties();
        if (args.length > 1) {
            properties.load(new FileReader(args[1]));
        } else {
            properties.load(ClassLoader.getSystemResourceAsStream("Config.properties"));
        }

        String username = properties.getProperty("eventhubspout.username");
        String password = properties.getProperty("eventhubspout.password");
        String namespaceName = properties
                .getProperty("eventhubspout.namespace");
        String entityPath = properties.getProperty("eventhubspout.entitypath");
        String zkEndpointAddress = properties
                .getProperty("zookeeper.connectionstring"); // opt


        int partitionCount = Integer.parseInt(properties
                .getProperty("eventhubspout.partitions.count"));
        int checkpointIntervalInSeconds = Integer.parseInt(properties
                .getProperty("eventhubspout.checkpoint.interval"));
        int receiverCredits = Integer.parseInt(properties
                .getProperty("eventhub.receiver.credits")); // prefetch count
        // (opt)
        System.out.println("Eventhub spout config: ");
        System.out.println("  partition count: " + partitionCount);
        System.out.println("  checkpoint interval: "
                + checkpointIntervalInSeconds);
        System.out.println("  receiver credits: " + receiverCredits);

        spoutConfig = new EventHubSpoutConfig(username, password,
                namespaceName, entityPath, partitionCount, zkEndpointAddress,
                checkpointIntervalInSeconds, receiverCredits);

        // set the number of workers to be the same as partition number.
        // the idea is to have a spout and a logger bolt co-exist in one
        // worker to avoid shuffling messages across workers in storm cluster.
        numWorkers = spoutConfig.getPartitionCount();
        System.out.println("numWorkers: " + numWorkers);

        if (args.length > 0) {
            // set topology name so that sample Trident topology can use it as
            // stream name.
            spoutConfig.setTopologyName(args[0]);
        }
        System.out.println("Completed reading configuration");
    }

    protected StormTopology buildTopology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        EventHubSpout eventHubSpout = new EventHubSpout(spoutConfig);
        topologyBuilder.setSpout("EventHubsSpout", eventHubSpout,
                spoutConfig.getPartitionCount()).setNumTasks(
                spoutConfig.getPartitionCount());
        topologyBuilder
                .setBolt("PowerBiBolt", new PowerBiBolt(),
                        spoutConfig.getPartitionCount())
                .globalGrouping("EventHubsSpout")
                .setNumTasks(spoutConfig.getPartitionCount());
        return topologyBuilder.createTopology();
    }

    protected void runScenario(String[] args) throws Exception {
        boolean runLocal = true;
        readEventHubConfig(args);
        StormTopology topology = buildTopology();
        Config config = new Config();
        config.setDebug(false);

        if (runLocal) {
            config.setMaxTaskParallelism(2);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("test", config, topology);
            Thread.sleep(5000000);
            localCluster.shutdown();
        } else {
            config.setNumWorkers(numWorkers);
            StormSubmitter.submitTopology(args[0], config, topology);
        }
    }

    public static void main(String[] args) throws Exception {
        LogTopology topology = new LogTopology();
        topology.runScenario(args);
    }
}