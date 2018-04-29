package com.yousry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yousry.Config.ConfigValues;
import com.yousry.Helpers.CountryCodeMapper;
import com.yousry.Helpers.HttpHelper;
import com.yousry.PowerBi.PowerBiPayload;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class PowerBiBolt extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(PowerBiBolt.class);
    private static final CountryCodeMapper countryNameMapper = new CountryCodeMapper();
    private Map<String,Integer> globalMap = new HashMap<String,Integer>();

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        return conf;
    }

    public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
        logger.info("Execute is called");
        try {
            if (isTickTuple(tuple)) {
                submitSummaryToPowerBi(globalMap);
                return;
            }

            Map<String,Integer> localMap = (Map<String,Integer>)tuple.getValueByField("gdeltDataSummary");
            accumulateEntries(localMap);
            logger.info("GDELT data processed");
        } catch (Exception e) {
            logger.error("Bolt execute error: {}", e);
        }
    }

    private void accumulateEntries(Map<String, Integer> localMap) {
        for (Map.Entry<String, Integer> entry : localMap.entrySet()) {
            String key = entry.getKey();
            if (globalMap.containsKey(key))
            {
                globalMap.put(key, globalMap.get(key) + entry.getValue());
            }
            else
            {
                globalMap.put(key, entry.getValue());
            }
        }
    }

    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // no output fields
    }

    public void submitSummaryToPowerBi(Map<String,Integer> map) throws IOException {
        HttpHelper helper = new HttpHelper();
        ObjectMapper mapper = new ObjectMapper();

        List<PowerBiPayload> countrySummary = GetTop10Countries(map);
        String stringData = mapper.writeValueAsString(countrySummary);
        logger.info("Submitting to PowerBI: {}" , stringData);
        int responseCode = helper.post(ConfigValues.powerBiPushUrl, stringData);
        logger.info("Response from PowerBI: {}" , responseCode);
    }

    public static List<PowerBiPayload> GetTop10Countries(Map<String,Integer> map)
    {
        PowerBiPayload[] blobs = new PowerBiPayload[map.size()];
        int index = 0;

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            PowerBiPayload payload = new PowerBiPayload();
            payload.Country = countryNameMapper.GetCountryName(entry.getKey());
            payload.Count = entry.getValue();
            blobs[index++] = payload;
        }

        Arrays.sort(blobs, (a, b) -> b.Count - a.Count); // descending sort

        List<PowerBiPayload> top10 =  Arrays.stream(blobs).limit(10).collect(Collectors.toList());

        return top10;
    }
}