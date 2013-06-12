//
// Author: Mark Triggs <mark@dishevelled.net>
//

import java.io.*;
import java.util.*;

import java.sql.*;

import org.apache.commons.codec.binary.Base64;
// Note that this version is coming from Solr!
//import org.apache.commons.codec.binary.Base64;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.FSDirectory;

import au.gov.nla.util.BrowseEntry;


public class CreateBrowseSQLite
{
    private Connection outputDB;

    private Leech bibLeech;
    private Leech authLeech;
    private Leech nonprefAuthLeech;

    IndexSearcher bibSearcher;
    IndexSearcher authSearcher;

    private String luceneField;

    private void loadHeadings (Leech leech, Predicate predicate)
        throws Exception
    {
        BrowseEntry h;
        int count = 0;
        int id = 0;

        System.out.println("Loading headings...");
        
        outputDB.setAutoCommit (false);

        PreparedStatement prepGetHeading = outputDB.prepareStatement (
             "select id from all_headings where key = ?");

        PreparedStatement prepAddHeading = outputDB.prepareStatement (
            "insert or ignore into all_headings (id, key, heading) values (?, ?, ?)");

        PreparedStatement prepGetFilterValue = outputDB.prepareStatement (
            "select id from filter_value where type_id=? and value=?");
        
        PreparedStatement prepAddFilterValue = outputDB.prepareStatement (
            "insert into filter_value (id, type_id, value) values (?, ?, ?)");

        PreparedStatement prepAddFilterLink = outputDB.prepareStatement (
            "insert or ignore into filter_link (heading_id, filter_value_id) values (?, ?)");

        HashMap<String, Integer> filterTypeMap = new HashMap<String, Integer>();
        HashMap<String, Integer> headingIdCache = new LinkedHashMap<String, Integer>(50000, .75F, true) {
            protected static final long serialVersionUID = 1L;
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() >= 50000;
            }
        };
        HashMap<String, Integer> filterValueCache = new LinkedHashMap<String, Integer>(10000, .75F, true) {
            protected static final long serialVersionUID = 1L;
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() >= 10000;
            }
        };
        int maxValueId = 0;
        int linkCount = 0;
        
        while ((h = leech.next ()) != null) {
            if (predicate != null &&
                !predicate.isSatisfiedBy (h.value)) {
                continue;
            }

            if (h.key != null) {
                String headingKeyStr = new String (Base64.encodeBase64 (h.key));
                int headingId;
                if (headingIdCache.containsKey (headingKeyStr)) {
                    headingId = headingIdCache.get(headingKeyStr);
                } else {                
                    prepGetHeading.setBytes (1, h.key); 
                    ResultSet rs = prepGetHeading.executeQuery ();
                    if (rs.next ()) {
                        headingId = rs.getInt ("id");
                    } else {
                        ++id;
                        prepAddHeading.setInt (1,  id); 
                        prepAddHeading.setBytes (2, h.key);
                        prepAddHeading.setString (3, h.value);
                        prepAddHeading.execute();
                        headingId = id;
                    }
                    rs.close();
                    headingIdCache.put (headingKeyStr, headingId);
                }
                 
                // Add filters and store filter types so that we can build 
                // filter_type table in the end
                if (h.filters != null) {
                    for (String filterType: h.filters.keySet ()) {
                        String[] values = h.filters.get (filterType);
                        for (String filterValue: values) {
                            if (!filterTypeMap.containsKey (filterType)) {
                                filterTypeMap.put (filterType, filterTypeMap.size () + 1);
                            }
                            int typeId = filterTypeMap.get (filterType);
                              
                            int currentValueId = 0;

                            if (filterValueCache.containsKey(filterType + ":" + filterValue)) {
                                currentValueId = filterValueCache.get(filterType + ":" + filterValue);
                            } else {
                                prepGetFilterValue.setInt(1, typeId);
                                prepGetFilterValue.setString(2, filterValue);
                                ResultSet filterValueRs = prepGetFilterValue.executeQuery ();
                                if (filterValueRs.next ()) {
                                    currentValueId = filterValueRs.getInt ("id");
                                } else {
                                    prepAddFilterValue.setInt (1, ++maxValueId);
                                    prepAddFilterValue.setInt (2, typeId);
                                    prepAddFilterValue.setString (3, filterValue);
                                    prepAddFilterValue.execute ();
                                    
                                    currentValueId = maxValueId;
                                }
                                filterValueCache.put (filterType + ":" + filterValue, currentValueId);
                            }
                            prepAddFilterLink.setInt (1, id);
                            prepAddFilterLink.setInt (2, currentValueId);
                            prepAddFilterLink.addBatch ();

                            if ((++linkCount % 500000) == 0) {
                                prepAddFilterLink.executeBatch ();
                                prepAddFilterLink.clearBatch ();
                            }
                        }
                    }
                }
            }

            count++;

            if ((count % 100000) == 0) {
                outputDB.commit ();
                System.out.println(new Integer(count) + " headings loaded");
            }
        }

        prepAddFilterLink.executeBatch ();
        prepAddFilterLink.close ();
        
        System.out.println(new Integer(count) + " headings loaded");
        
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

    private String getEnvironment (String var)
    {
        return (System.getenv (var) != null) ?
            System.getenv (var) : System.getProperty (var.toLowerCase ());
    }
    
    private int bibCount (String heading) throws IOException
    {
        TotalHitCountCollector counter = new TotalHitCountCollector();

        bibSearcher.search (new ConstantScoreQuery(new TermQuery (new Term (luceneField, heading))),
                            counter);

        return counter.getTotalHits ();
    }
    
    private boolean isLinkedFromBibData (String heading)
            throws IOException
    {
        TopDocs hits = null;

        int max_headings = 20;
        while (true) {
            hits = authSearcher.search
                (new ConstantScoreQuery
                 (new TermQuery
                  (new Term
                   (System.getProperty ("field.insteadof", "insteadOf"),
                    heading))),
                 max_headings);

            if (hits.scoreDocs.length < max_headings) {
                // That's all of them.  All done.
                break;
            } else {
                // Hm.  That's a lot of headings.  Go back for more.
                max_headings *= 2;
            }
        }

        for (int i = 0; i < hits.scoreDocs.length; i++) {
            Document doc = authSearcher.getIndexReader ().document (hits.scoreDocs[i].doc);

            String[] preferred = doc.getValues (System.getProperty ("field.preferred", "preferred"));
            if (preferred.length > 0) {
                String preferredHeading = preferred[0];

                if (bibCount (preferredHeading) > 0) {
                    return true;
                }
            } else {
                return false;
            }
        }

        return false;
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
        stat.executeUpdate ("create index allkeyindex on all_headings (key);");
        stat.execute ("PRAGMA synchronous = OFF;");
        stat.execute ("PRAGMA journal_mode = OFF;");
        stat.execute ("PRAGMA locking_mode = EXCLUSIVE;");

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

    private Leech getBibLeech (String bibPath, String luceneField)
            throws Exception
    {
        String leechClass = "Leech";

        if (getEnvironment ("BIBLEECH") != null) {
            leechClass = getEnvironment ("BIBLEECH");
        }

        return (Leech) (Class.forName (leechClass)
                        .getConstructor (String.class, String.class)
                        .newInstance (bibPath, luceneField ));
    }
    
    public void create (String bibPath,
                        String luceneField,
                        String authPath, 
                        String outputPath)
        throws Exception
    {
        Class.forName ("org.sqlite.JDBC");
        outputDB = DriverManager.getConnection ("jdbc:sqlite:" + outputPath);

        setupDatabase ();

        this.luceneField = luceneField;
        bibLeech = getBibLeech (bibPath, luceneField);

        IndexReader bibReader = DirectoryReader.open (FSDirectory.open (new File (bibPath)));
        bibSearcher = new IndexSearcher (bibReader);

        if (authPath != null) {
            nonprefAuthLeech = new Leech (authPath,
                                          System.getProperty ("field.insteadof",
                                                              "insteadOf"));

            IndexReader authReader = DirectoryReader.open (FSDirectory.open (new File (authPath)));
            authSearcher = new IndexSearcher (authReader);

            loadHeadings (nonprefAuthLeech,
                          new Predicate () {
                              public boolean isSatisfiedBy (Object obj)
                              {
                                  String heading = (String) obj;

                                  try {
                                      return isLinkedFromBibData (heading);
                                  } catch (IOException e) {
                                      return true;
                                  }
                              }}
                );

            nonprefAuthLeech.dropOff ();
        }
        
        loadHeadings (bibLeech, null);

        buildOrderedTables ();
        dropAllHeadingsTable ();
        compactDatabase ();
        createFilterIndexes ();
    }


    public static void main (String args[])
        throws Exception
    {
        if (args.length != 3 && args.length != 4) {
            System.err.println
                ("Usage: CreateBrowseSQLite <bib index> <bib field> "
                 + "<auth index> <db file>");
            System.err.println ("\nor:\n");
            System.err.println
                ("Usage: CreateBrowseSQLite <bib index> <bib field>"
                 + " <db file>");
            System.exit (0);
        }

        CreateBrowseSQLite self = new CreateBrowseSQLite ();

        if (args.length == 4) {
            self.create (args[0], args[1], args[2], args[3]);
        } else {
            self.create (args[0], args[1], null, args[2]);
        }
    }
}
