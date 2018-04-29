package com.yousry.HBase;

import com.fasterxml.jackson.annotation.JsonProperty;

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
    @JsonProperty(value = "$")
    public String Value;
    public String column;
}