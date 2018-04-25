package com.yousry.HBase;

public class HBaseRecord {
    public int Day;
    public int Month;
    public int Year;
    public float GoldsteinScale;
    public String Country; // ActionGeo_CountryCode ==> will be mapped in Storm to country name
    public String Actor1Name;
    public String Actor2Name;
}
