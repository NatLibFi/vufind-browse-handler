// Build a browse list by walking the docs in an index and extracting sort key
// and values from a pair of stored fields.

import java.io.*;
import java.util.*;
import org.apache.lucene.store.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;

import au.gov.nla.util.Utils;
import au.gov.nla.util.BrowseEntry;

public class StoredFieldLeech extends Leech
{
    int currentDoc = 0;
    LinkedList<BrowseEntry> buffer;

    String[] sortFields;
    String[] valueFields;
    String[] filterFields;

    private Set<String> fieldSelection;


    public StoredFieldLeech (String indexPath, String field) throws Exception
    {
        super (indexPath, field);

        sortFields = Utils.getEnvironment ("SORTFIELD").split(":");
        valueFields = Utils.getEnvironment ("VALUEFIELD").split(":");
        filterFields = Utils.getEnvironment ("FILTERFIELD").split(":");

        if (sortFields.length == 0 || valueFields.length == 0) {
            throw new IllegalArgumentException ("Both SORTFIELD and " +
                                                "VALUEFIELD environment " +
                                                "variables must be set.");
        }

        fieldSelection = new HashSet<String>();
        for (String fld: sortFields) {
            fieldSelection.add (fld);
        }
        for (String fld: valueFields) {
            fieldSelection.add (fld);
        }
        for (String fld: filterFields) {
            fieldSelection.add (fld);
        }

        reader = DirectoryReader.open (FSDirectory.open (new File (indexPath)));
        buffer = new LinkedList<BrowseEntry> ();
    }


    private void loadDocument (IndexReader reader, int docid)
        throws Exception
    {
        Document doc = reader.document (currentDoc, fieldSelection);

        List<String> sort_keys = new LinkedList<String> ();
        for (String fld: sortFields) {
            for (String value: doc.getValues (fld)) {
                sort_keys.add (value);
            }
        }
        List<String> values = new LinkedList<String> ();
        for (String fld: valueFields) {
            for (String value: doc.getValues (fld)) {
                values.add (value);
            }
        }
        Map<String, String[]> filterMap = new HashMap<String, String[]> ();
        for (String fld: filterFields) {
            filterMap.put (fld, doc.getValues (fld));
        }

        if (sort_keys.size() == values.size()) {
            for (int i = 0; i < values.size(); i++) {
                buffer.add (new BrowseEntry(buildSortKey(sort_keys.get(i)), values.get(i), filterMap));
            }
        } else {
            System.err.println("Skipped entries for docid " + docid +
                               " because the number of sort keys didn't" +
                               " match the number of stored values.");
        }
    }


    public BrowseEntry next () throws Exception
    {
        while (buffer.isEmpty ()) {
            if (currentDoc < reader.maxDoc ()) {
                loadDocument (reader, currentDoc);
                currentDoc++;
            } else {
                return null;
            }
        }

        return buffer.remove ();
    }
}

