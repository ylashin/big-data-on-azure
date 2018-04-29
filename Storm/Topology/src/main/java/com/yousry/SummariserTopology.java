package com.yousry;

import com.yousry.Config.ConfigValues;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.eventhubs.spout.EventHubSpout;
import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;


public class SummariserTopology {
    protected EventHubSpoutConfig spoutConfig;

    protected void setupEventHubConfig() throws Exception {
        spoutConfig = new EventHubSpoutConfig(
                ConfigValues.eventHubUserName,
                ConfigValues.eventHubPassword,
                ConfigValues.eventHubNamespaceName,
                ConfigValues.eventHubEntityPath,
                ConfigValues.eventHubPartitionCount,
                ConfigValues.eventHubZooKeeperEndpointAddress,
                ConfigValues.eventHubCheckpointIntervalInSeconds,
                ConfigValues.eventHubReceiverCredits);

    }

    protected StormTopology buildTopology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        EventHubSpout eventHubSpout = new EventHubSpout(spoutConfig);
        topologyBuilder.setSpout("EventHubsSpout", eventHubSpout, spoutConfig.getPartitionCount())
                .setNumTasks(spoutConfig.getPartitionCount());

        topologyBuilder
                .setBolt("SummariseBolt", new SummariseBolt(), spoutConfig.getPartitionCount())
                .shuffleGrouping("EventHubsSpout")
                .setNumTasks(spoutConfig.getPartitionCount());


        topologyBuilder
                .setBolt("PowerBiBolt", new PowerBiBolt(), 1 ) // global bolt
                .globalGrouping("SummariseBolt")
                .setNumTasks(1);

        return topologyBuilder.createTopology();
    }

    protected void runScenario(String topologyName) throws Exception {
        boolean runLocal = false;
        setupEventHubConfig();
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
            int workerCount  = spoutConfig.getPartitionCount();
            config.setNumWorkers(workerCount);
            StormSubmitter.submitTopology(topologyName, config, topology);
        }
    }

    public static void main(String[] args) throws Exception {
        SummariserTopology topology = new SummariserTopology();
        String topologyName = args.length > 0 ? args[0] : UUID.randomUUID().toString().substring(24);
        topology.runScenario(topologyName);
    }
}