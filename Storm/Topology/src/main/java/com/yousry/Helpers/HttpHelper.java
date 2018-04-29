package com.yousry.Helpers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpHelper {

    private static final Logger logger = LoggerFactory.getLogger(HttpHelper.class);

    public int post(String url, String payload)
    {
        return post(url, payload, "");
    }

    public int post(String url, String payload, String authHeaderValue)
    {
        URL urlObj = null;
        HttpURLConnection con = null;
        try {
            urlObj = new URL(url);
            con = (HttpURLConnection) urlObj.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("accept", "application/json");
            con.setRequestProperty("User-Agent", "Hadoop-Cluster");
            if (authHeaderValue != "")
                con.setRequestProperty("authorization", authHeaderValue);

            con.setUseCaches(false);
            con.setDoInput(true);
            con.setDoOutput(true);


            DataOutputStream out = new DataOutputStream(con.getOutputStream());
            out.writeBytes(payload);
            out.flush();
            out.close();


            int responseCode = con.getResponseCode();
            return responseCode;

        } catch (Exception e) {
            logger.error("HttpHelper: error in POST call: {}", e);
            return -1;
        }
        finally {
            if (con != null)
                con.disconnect();
        }

    }
}
