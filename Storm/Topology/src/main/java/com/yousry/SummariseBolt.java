package com.yousry;

import com.yousry.HBase.HBaseHelper;
import com.yousry.HBase.HBaseRecord;
import com.yousry.Helpers.CountryCodeMapper;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class SummariseBolt extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(SummariseBolt.class);
    private static final CountryCodeMapper countryNameMapper = new CountryCodeMapper();
    private HBaseHelper hbaseHelper;

    private Map<String,Integer> dataMap = new HashMap<String,Integer>();
    private ArrayList<HBaseRecord> records = new ArrayList<HBaseRecord>();

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        return conf;
    }

    public void prepare(Map map, TopologyContext context) {

        hbaseHelper = new HBaseHelper();
    }

    public void execute(Tuple tuple , BasicOutputCollector outputCollector) {
        try {
            if (isTickTuple(tuple)) {
                outputCollector.emit(new Values(dataMap));
                dataMap.clear();
                return;
            }

            String value = tuple.getString(0);
            if (value==null)
                return;

            logger.info("Before processing event hub message of content length : " + value.length());
            String[] lines = value.split("\\r?\\n");
            summarizeLines(lines);
            logger.info("GDELT data processed " + lines.length + " lines");
        } catch (Exception e) {
            logger.error("Bolt execute error: {}", e);
        }

    }

    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    private void summarizeLines(String[] lines) {
        logger.info("LineCount for current tuple:" + lines.length);

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

            prepareHBaseRecord(values, countryCode);
        }

        try {
            hbaseHelper.SaveBatch(records);
            logger.info("Yay, saved " + records.size() + " hbase records");
        }
        catch (Exception e) {
            logger.error("cannot save hbase records: {}", e);
        }
        finally {
            // just ignoring HBase save failures for this simplistic implementation
            records.clear();
        }
    }

    private void prepareHBaseRecord(String[] values, String countryCode) {
        HBaseRecord record = new HBaseRecord();
        record.Country = countryNameMapper.GetCountryName(countryCode);
        String fullDay = values[1].trim(); // in format 19790101
        record.Day = Integer.parseInt(fullDay.substring(6,8));
        record.Month = Integer.parseInt(fullDay.substring(4,6));
        record.Year = Integer.parseInt(fullDay.substring(0,4));
        record.GoldsteinScale = Float.parseFloat(values[30]);
        record.Actor1Name = values[6];
        record.Actor2Name = values[16];

        records.add(record);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("gdeltDataSummary"));
    }
}