//
// Author: Mark Triggs <mark@dishevelled.net>
//

import java.io.*;
import java.util.*;

import java.sql.*;

// Note that this version is coming from Solr!
import org.apache.commons.codec.binary.Base64;


public class CreateBrowseSQLite
{
    private Connection outputDB;

    private String KEY_SEPARATOR = "\1";
    private String FILTER_SEPARATOR = "\2";


    /*
     * Like BufferedReader#readLine(), but only returns lines ended by a \r\n.
     */
    private String readCRLFLine (BufferedReader br) throws IOException
    {
        StringBuilder sb = new StringBuilder();

        while (true) {
            int ch = br.read ();

            if (ch >= 0) {
                if (ch == '\r') {
                    // This might either be a carriage return embedded in record
                    // data (which we want to preserve) or the first part of the
                    // \r\n end of line marker.

                    ch = br.read ();

                    if (ch == '\n') {
                        // An end of line.  We're done.
                        return sb.toString();
                    }

                    // Must have been an embedded carriage return.  Keep it.
                    sb.append('\r');
                }

                sb.append((char) ch);
            } else {
                // EOF.  Show's over.
                return null;
            }
        }
    }


    private void loadHeadings (BufferedReader br)
        throws Exception
    {
        int count = 0;
        int id = 0;

        System.out.println("Loading headings...");
        
        outputDB.setAutoCommit (false);

        PreparedStatement prepAddHeading = outputDB.prepareStatement (
            "insert or ignore into all_headings (id, key, heading) values (?, ?, ?)");

        PreparedStatement prepGetFilterValue = outputDB.prepareStatement (
                "select id from filter_value where type_id=? and value=?");
        
        PreparedStatement prepAddFilterValue = outputDB.prepareStatement (
                "insert into filter_value (id, type_id, value) values (?, ?, ?)");

        PreparedStatement prepAddFilterLink = outputDB.prepareStatement (
                "insert or ignore into filter_link (heading_id, filter_value_id) values (?, ?)");

        HashMap<String, Integer> filterTypeMap = new HashMap<String, Integer>();
        HashMap<String, Integer> filterValueCache = new LinkedHashMap<String, Integer>(1000, .75F, true) {
            protected static final long serialVersionUID = 1L;
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() >= 1000;
            }
        };
        String line;
        String prevHeading = null;
        int maxValueId = 0;
        int linkCount = 0;
        while ((line = readCRLFLine (br)) != null) {
            String[] elements = line.split(KEY_SEPARATOR);

            if (elements.length > 1) {

                byte[] key = Base64.decodeBase64 (elements[0].getBytes());
                String heading = elements[1];

                if (prevHeading == null || !prevHeading.equals (heading)) {
                    ++id;
                    prepAddHeading.setInt (1,  id); 
                    prepAddHeading.setBytes (2, key);
                    prepAddHeading.setString (3, heading);
                    prepAddHeading.addBatch ();
                }
                prevHeading = heading;
                
                // Add filters and store filter types so that we can build 
                // filter_type table in the end
                if (elements.length > 2) {
                    for (String filter: elements[2].split (FILTER_SEPARATOR)) {
                        int filterSep = filter.indexOf (':');
                        if (filterSep > 0) {
                            String filterType = filter.substring (0, filterSep);
                            String filterValue = filter.substring (filterSep + 1);
                            if (!filterTypeMap.containsKey (filterType)) {
                                filterTypeMap.put (filterType, filterTypeMap.size () + 1);
                            }
                            int typeId = filterTypeMap.get (filterType);
                              
                            int currentValueId = 0;

                            if (filterValueCache.containsKey(filter)) {
                                currentValueId = filterValueCache.get(filter);
                            } else {
                                prepGetFilterValue.setInt(1, typeId);
                                prepGetFilterValue.setString(2, filterValue);
                                ResultSet filterValueRs = prepGetFilterValue.executeQuery ();
                                if (filterValueRs.next()) {
                                    currentValueId = filterValueRs.getInt("id");
                                } else {
                                    prepAddFilterValue.setInt (1, ++maxValueId);
                                    prepAddFilterValue.setInt (2, typeId);
                                    prepAddFilterValue.setString (3, filterValue);
                                    prepAddFilterValue.execute ();
                                    
                                    currentValueId = maxValueId;
                                }
                                filterValueCache.put(filter, currentValueId);
                            }
                            prepAddFilterLink.setInt(1, id);
                            prepAddFilterLink.setInt(2, currentValueId);
                            prepAddFilterLink.addBatch ();

                            if ((++linkCount % 500000) == 0) {
                                System.out.print("  Executing link batch...");
                                prepAddFilterLink.executeBatch ();
                                prepAddFilterLink.clearBatch ();
                                System.out.println(" done");
                            }
                        } else {
                              System.err.println ("Invalid filter string: '" + filter + "'");
                        }
                    }
                }
            }

            if ((count % 500000) == 0) {
                prepAddHeading.executeBatch ();
                prepAddHeading.clearBatch ();
            }
            
            count++;
        }

        prepAddHeading.executeBatch ();
        prepAddHeading.close ();
        prepAddFilterLink.executeBatch ();
        prepAddFilterLink.close ();
        
        // Build filter_type table
        System.out.println("Building filter type table...");
        PreparedStatement prepAddFilterType = outputDB.prepareStatement (
                "insert into filter_type (id, type) values (?, ?)");
        
        for (String typeKey: filterTypeMap.keySet ()) {
              prepAddFilterType.setInt(1, filterTypeMap.get (typeKey));
              prepAddFilterType.setString (2, typeKey);
              prepAddFilterType.execute ();
        }
        
        prepAddFilterType.close ();

        outputDB.commit ();
        outputDB.setAutoCommit (true);
    }


    private void setupDatabase ()
        throws Exception
    {
        Statement stat = outputDB.createStatement ();

        stat.executeUpdate ("drop table if exists all_headings;");
        stat.executeUpdate ("drop table if exists filter_type;");
        stat.executeUpdate ("drop table if exists filter_value;");
        stat.executeUpdate ("drop table if exists filter_link;");
        stat.executeUpdate ("create table all_headings (id, key, heading);");
        stat.executeUpdate ("create table filter_type (id, type);");
        stat.executeUpdate ("create table filter_value (id, type_id, value);");
        stat.executeUpdate ("create table filter_link (heading_id, filter_value_id, primary key (heading_id, filter_value_id));");
        stat.executeUpdate ("PRAGMA synchronous = OFF;");
        stat.execute ("PRAGMA journal_mode = OFF;");

        stat.close ();
    }

    // Hoping this helps to build large databases
    private void createKeyIndex ()
        throws Exception
    {
        System.out.println("Creating all_headings key index...");
        
        Statement stat = outputDB.createStatement ();
        stat.executeUpdate ("create index allkeyindex on all_headings (key);");
        stat.close ();
    }

    private void buildOrderedTables ()
        throws Exception
    {
        System.out.println("Building ordered tables...");
        Statement stat = outputDB.createStatement ();

        stat.executeUpdate ("drop table if exists headings;");
        stat.executeUpdate ("create table headings " +
                            "as select * from all_headings order by key;");

        stat.executeUpdate ("create index keyindex on headings (key);");

        stat.close ();
    }

    private void dropAllHeadingsTable ()
        throws Exception
    {
        System.out.println("Dropping temporary tables...");
        Statement stat = outputDB.createStatement ();

        stat.executeUpdate ("drop table if exists all_headings;");
        stat.close ();
    }

    private void createFilterIndexes ()
            throws Exception
    {
        Statement stat = outputDB.createStatement ();
        System.out.println("Creating filter indexes...");

        stat.executeUpdate ("create index filter_link_covering2 on filter_link(filter_value_id, heading_id);");
        stat.executeUpdate ("create index filter_value_covering on filter_value(id, type_id, value);");
        stat.close ();
    }

    private void compactDatabase ()
            throws Exception
    {
        Statement stat = outputDB.createStatement ();
        System.out.println("Compacting database...");

        stat.executeUpdate ("vacuum;");
        stat.close ();
    }

    public void create (String headingsFile, String outputPath)
        throws Exception
    {
        Class.forName ("org.sqlite.JDBC");
        outputDB = DriverManager.getConnection ("jdbc:sqlite:" + outputPath);

        setupDatabase ();

        BufferedReader br = new BufferedReader
            (new FileReader (headingsFile));

        loadHeadings (br);

        br.close ();

        createKeyIndex ();
        buildOrderedTables ();
        dropAllHeadingsTable ();
        createFilterIndexes ();
        compactDatabase();
    }


    public static void main (String args[])
        throws Exception
    {
        if (args.length != 2) {
            System.err.println
                ("Usage: CreateBrowseSQLite <headings file> <db file>");
            System.exit (0);
        }

        CreateBrowseSQLite self = new CreateBrowseSQLite ();

        self.create (args[0], args[1]);
    }
}
