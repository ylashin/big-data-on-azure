package com.yousry;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;


public class SummariseBolt extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(SummariseBolt.class);
    private Map<String,Integer> dataMap = new HashMap<String,Integer>();

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        return conf;
    }

    public void execute(Tuple tuple , BasicOutputCollector outputCollector) {
        logger.info("Summarise Bolt: execute is called");
        try {
            if (isTickTuple(tuple)) {
                outputCollector.emit(new Values(dataMap));
                dataMap.clear();
                logger.info("Summarise Bolt: Timer processed");
                return;
            }

            String value = tuple.getString(0);
            if (value!=null)
                logger.info("Summarise Bolt: Before processing GDELT, content length : " + value.length());

            String[] lines = value.split("\\r?\\n");
            summarizeLines(lines);
            logger.info("Summarise Bolt: GDELT data processed");
        } catch (Exception e) {
            logger.error("Summarise Bolt: Bolt execute error: {}", e);
        }

    }

    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    private void summarizeLines(String[] lines) {
        for(int i=0;i<lines.length;i++)
        {
            String[] values = lines[i].split("\\t");

            String countryCode = values[51]; // action country code position

            if (countryCode == null || countryCode.trim() == "")
                countryCode = values[37]; // actor 1 country code position

            if (dataMap.containsKey(countryCode))
                dataMap.put(countryCode,dataMap.get(countryCode) + 1);
            else
                dataMap.put(countryCode,1);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("gdeltDataSummary"));
    }
}