package com.yousry;

import com.google.gson.Gson;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.TopologyContext;
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
    private String powerBiPushUrl;

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        return conf;
    }

    public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
        logger.info("PowerBiBolt: execute is called");
        try {
            if (isTickTuple(tuple)) {
                submitSummaryToPowerBi(globalMap);
                logger.info("PowerBiBolt: Timer processed");
                return;
            }

            Map<String,Integer> localMap = (Map<String,Integer>)tuple.getValueByField("gdeltDataSummary");
            accumulateEntries(localMap);
            logger.info("PowerBiBolt: GDELT data processed");
        } catch (Exception e) {
            logger.error("PowerBiBolt: Bolt execute error: {}", e);
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

    public void prepare(Map map, TopologyContext context) {
        Properties properties = new Properties();
        try {
            properties.load(ClassLoader.getSystemResourceAsStream("Config.properties"));
            powerBiPushUrl = properties.getProperty("powerbi.push.url");
            System.out.println("PowerBI Push RUL : " + powerBiPushUrl); // ignore security for now

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // no output fields
    }

    public void submitSummaryToPowerBi(Map<String,Integer> map) throws IOException {
        HttpHelper helper = new HttpHelper();
        Gson gson = new Gson();
        String stringData = gson.toJson(GetTop10Countries(map).toArray());
        logger.info("Submitting to PowerBI: " + stringData);
        int responseCode = helper.post(powerBiPushUrl, stringData);
        logger.info("Response from PowerBI: " + responseCode);
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