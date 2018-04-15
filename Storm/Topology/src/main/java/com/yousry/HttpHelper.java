package com.yousry;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpHelper {

    public int post(String url, String payload)
    {
        URL urlObj = null;
        HttpURLConnection con = null;
        try {
            urlObj = new URL(url);
            con = (HttpURLConnection) urlObj.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("User-Agent", "Storm-Cluster");

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
            e.printStackTrace();
            return -1;
        }
        finally {
            if (con != null)
                con.disconnect();
        }

    }
}
