package com.yousry.Helpers;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CountryCodeMapper {

    private static Map<String,String> countryMap = new HashMap<String,String>();

    static
    {
           try {
            String contents = IOUtils.toString(ClassLoader.getSystemResourceAsStream("Countries.csv"));
            CSVParser parser = CSVParser.parse(contents, CSVFormat.DEFAULT);
            for (CSVRecord csvRecord : parser) {
                countryMap.put(csvRecord.get(0),csvRecord.get(2));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String GetCountryName(String code)
    {
        if (countryMap.containsKey(code))
            return countryMap.get(code);

        return "Unknown";
    }
}
