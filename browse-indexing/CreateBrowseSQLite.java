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

        outputDB.setAutoCommit (false);

        PreparedStatement prep = outputDB.prepareStatement (
            "insert or ignore into all_headings (key, heading, building) values (?, ?, ?)");

        String line;
        while ((line = readCRLFLine (br)) != null) {
            int sep = line.indexOf (KEY_SEPARATOR.charAt (0));
            if (sep >= 0) {

                byte[] key = Base64.decodeBase64 (line.substring (0, sep).getBytes());
                prep.setBytes (1, key);

                int sep2 = line.indexOf (KEY_SEPARATOR.charAt (0), sep + 1);
                prep.setString (2, line.substring (sep + 1, sep2));
                prep.setString (3, line.substring (sep2 + 1));

                prep.addBatch ();
            }

            if ((count % 500000) == 0) {
                prep.executeBatch ();
                prep.clearBatch ();
            }

            count++;
        }

        prep.executeBatch ();
        prep.close ();

        outputDB.commit ();
        outputDB.setAutoCommit (true);
    }


    private void setupDatabase ()
        throws Exception
    {
        Statement stat = outputDB.createStatement ();

        stat.executeUpdate ("drop table if exists all_headings;");
        stat.executeUpdate ("create table all_headings (key, heading, building);");
        stat.executeUpdate ("PRAGMA synchronous = OFF;");
        stat.execute ("PRAGMA journal_mode = OFF;");

        stat.close ();
    }

    // Hoping this helps to build large databases
    private void createKeyIndex ()
        throws Exception
    {
        Statement stat = outputDB.createStatement ();
        stat.executeUpdate ("create index allkeyindex on all_headings (key);");
        stat.close ();
    }

    private void buildOrderedTables ()
        throws Exception
    {
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
        Statement stat = outputDB.createStatement ();

        stat.executeUpdate ("drop table if exists all_headings;");
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
