package com.yousry.Config;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class ConfigValues {
    //Power BI
    public static String powerBiPushUrl;
    // HBase
    public static String hbaseClusterRestEndpoint;
    public static String hbaseTableName;
    public static String hbaseUserName;
    public static String hbasePassword;

    // EventHubs
    public static String eventHubUserName;
    public static String eventHubPassword;
    public static String eventHubNamespaceName;
    public static String eventHubEntityPath;
    public static String eventHubZooKeeperEndpointAddress;
    public static int eventHubPartitionCount;
    public static int eventHubCheckpointIntervalInSeconds;
    public static int eventHubReceiverCredits;

    private static Logger logger;

    static{
        Properties properties = new Properties();
        logger = LoggerFactory.getLogger(ConfigValues.class);
        try {
            properties.load(ClassLoader.getSystemResourceAsStream("Config.properties"));
            powerBiPushUrl = properties.getProperty("powerbi.push.url");

            hbaseClusterRestEndpoint = properties.getProperty("hbase.restEndPoint");
            hbaseTableName = properties.getProperty("hbase.tableName");
            hbaseUserName = properties.getProperty("hbase.userName");
            hbasePassword = properties.getProperty("hbase.password");


            eventHubUserName = properties.getProperty("eventhubspout.username");
            eventHubPassword = properties.getProperty("eventhubspout.password");
            eventHubNamespaceName = properties.getProperty("eventhubspout.namespace");
            eventHubEntityPath = properties.getProperty("eventhubspout.entitypath");
            eventHubZooKeeperEndpointAddress = properties.getProperty("zookeeper.connectionstring");


            eventHubPartitionCount = Integer.parseInt(properties
                    .getProperty("eventhubspout.partitions.count"));
            eventHubCheckpointIntervalInSeconds = Integer.parseInt(properties
                    .getProperty("eventhubspout.checkpoint.interval"));
            eventHubReceiverCredits = Integer.parseInt(properties
                    .getProperty("eventhub.receiver.credits")); // prefetch count

        } catch (IOException e) {
            logger.error("ConfigValues: Config reading error: {}", e);
        }
    }
}
