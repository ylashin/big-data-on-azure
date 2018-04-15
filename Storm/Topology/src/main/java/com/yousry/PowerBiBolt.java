package com.yousry;

import com.google.gson.Gson;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class PowerBiBolt extends BaseRichBolt {
    private OutputCollector collector;
    private static final Logger logger = LoggerFactory.getLogger(PowerBiBolt.class);
    private Map<String,Integer> dataMap = new HashMap<String,Integer>();
    private String powerBiPushUrl;

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        return conf;
    }

    public void execute(Tuple tuple) {
        logger.info("Bolt execute is called");
        try {
            if (isTickTuple(tuple)) {
                submitSummaryToPowerBi(dataMap);
                logger.info("Timer processed");
                return;
            }


            String value = tuple.getString(0);
            if (value!=null)
                logger.info("Before processing GDELT, content length : " + value.length());

            String[] lines = value.split("\\r?\\n");
            summarizeLines(lines);
            logger.info("GDELT data processed");
            collector.ack(tuple);

            // do your bolt stuff
        } catch (Exception e) {
            logger.error("Bolt execute error: {}", e);
            collector.reportError(e);
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

    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

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
        CountryCodeMapper mapper = new CountryCodeMapper();

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            PowerBiPayload payload = new PowerBiPayload();
            payload.Country = mapper.GetCountryName(entry.getKey());
            payload.Count = entry.getValue();
            blobs[index++] = payload;
        }

        Arrays.sort(blobs, (a, b) -> b.Count - a.Count); // descending sort

        List<PowerBiPayload> top10 =  Arrays.stream(blobs).limit(10).collect(Collectors.toList());

        return top10;
    }
}