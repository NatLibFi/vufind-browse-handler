package au.gov.nla.util;

import java.util.Map;

public class BrowseEntry
{
    public byte[] key;
    public String value;
    public Map<String, String[]> filters;

    public BrowseEntry (byte[] key, String value, Map<String, String[]> filters)
    {
        this.key = key;
        this.value = value;
        this.filters = filters;
    }
}
