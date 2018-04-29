package com.yousry.HBase;



import com.fasterxml.jackson.databind.ObjectMapper;
import com.yousry.Config.ConfigValues;
import com.yousry.Helpers.HttpHelper;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class HBaseHelper {

    private static final Logger logger = LoggerFactory.getLogger(HBaseHelper.class);

    public void SaveBatch(List<HBaseRecord> records) throws InterruptedException, IOException {

        ObjectMapper mapper = new ObjectMapper();

        String url = ConfigValues.hbaseClusterRestEndpoint + "/"+ ConfigValues.hbaseTableName +"/dummy";
        // HACK: hbase insert URL requires a row key and dummy value could be provided while the real row keys are in JSON payload

        String stringData = mapper.writeValueAsString(GetData(records));
        String credentials =  "Basic " + Base64.encodeBase64String((ConfigValues.hbaseUserName + ":" + ConfigValues.hbasePassword ).getBytes());
        HttpHelper helper = new HttpHelper();
        logger.info("Submitting to HBase : " + url);
        int responseCode = helper.post(url, stringData, credentials);
        logger.info("Response from HBase: " + responseCode);
    }

    public static Blob GetData(List<HBaseRecord> records)
    {
        Blob blob = new Blob();
        blob.Row = new Row[records.size()];
        for(int i=0; i<records.size(); i++)
        {
            Row row = new Row();
            blob.Row[i] = row;
            FillRow(records.get(i), row);
        }
        return blob;
    }

    private static void FillRow(HBaseRecord record, Row row) {

        String random = UUID.randomUUID().toString().substring(24);
        String key = String.format("%04d",record.Year) +
                String.format("%02d",record.Month) +
                String.format("%02d",record.Day) +
                "#" +
                random;

        row.key = Base64.encodeBase64String(key.getBytes());

        ArrayList<Cell> cellList = new ArrayList<Cell>();
        addCellValue(cellList,"time:day", Integer.toString(record.Day));
        addCellValue(cellList,"time:month", Integer.toString(record.Month));
        addCellValue(cellList,"time:year", Integer.toString(record.Year));
        addCellValue(cellList,"actor1:name", record.Actor1Name);
        addCellValue(cellList,"actor2:name", record.Actor2Name);
        addCellValue(cellList,"geo:country", record.Country);
        addCellValue(cellList,"attributes:goldsteinScale", Float.toString(record.GoldsteinScale));
        row.Cell = cellList.toArray(new Cell[cellList.size()]);
    }

    private static void addCellValue(ArrayList<Cell> cellList, String cellName, String cellValue) {
        Cell cell = new Cell();
        cell.column = Base64.encodeBase64String(cellName.getBytes());
        cell.Value = Base64.encodeBase64String(cellValue.getBytes());
        cellList.add(cell);
    }
}
