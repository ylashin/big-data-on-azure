package com.yousry.HBase;

import com.google.gson.annotations.SerializedName;

public class Blob
{
    public Row[] Row;
}

class Row
{
    public Cell[] Cell;
    public String key;
}

class Cell
{
    @SerializedName("$")
    public String Value;
    public String column;
}