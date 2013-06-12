//
// Author: Mark Triggs <mark@dishevelled.net>
//


package au.gov.nla.solr.handler;


import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.search.*;
import org.apache.lucene.queries.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.util.*;
import org.apache.solr.handler.*;
import org.apache.solr.parser.QueryParser;
import org.apache.solr.request.*;
import org.apache.solr.search.LuceneQParserPlugin;
import org.apache.solr.search.QParser;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import java.io.*;
import java.util.*;
import java.net.URL;
import java.sql.*;

import au.gov.nla.util.*;

import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import au.gov.nla.util.Normaliser;
import au.gov.nla.util.BrowseEntry;

class Log
{
    private static Logger log ()
    {
        // Caller's class
        return Logger.getLogger
            (new Throwable ().getStackTrace ()[2].getClassName ());
    }


    public static void info (String s) { log ().info (s); }
}


/*
 *
 * This class stores the list of headings retrieves from
 * a solr index.
 *
 * It also contains 
 *     startRow
 *        This rowid is used as the seed to browse previous
 *     endRow
 *        This rowid is used as the seed to browse forward
 *     total
 *        The total number of headings in the list
 *
 */
class HeadingSlice
{
    public LinkedList<String> headings = new LinkedList<String> ();
    public int startRow;
    public int endRow;
    public int total;
}


/*
 *
 * This is the interface to the sqlite database
 *
 */

class HeadingsDB
{
    Connection db;
    String path;
    long dbVersion;
    int totalCount;
    Normaliser normaliser;
    Map<String, Integer> filterTypeMap = null;

    ReentrantReadWriteLock dbLock = new ReentrantReadWriteLock ();

    public HeadingsDB (String path) throws Exception
    {
        this.path = path;
        normaliser = Normaliser.getInstance ();
    }


    private void openDB () throws Exception
    {
        if (!new File (path).exists()) {
            throw new Exception ("I couldn't find a browse index at: " + path +
                                 ".\nMaybe you need to create your browse indexes?");
        }

        Class.forName ("org.sqlite.JDBC");

        db = DriverManager.getConnection ("jdbc:sqlite:" + path);
        db.setAutoCommit (false);
        dbVersion = currentVersion ();

        PreparedStatement countStmnt = db.prepareStatement (
            "select count(1) as count from headings");

        ResultSet rs = countStmnt.executeQuery ();
        rs.next ();

        totalCount = rs.getInt ("count");

        rs.close ();
        countStmnt.close ();
    }


    private long currentVersion ()
    {
        return new File (path).lastModified ();
    }

    /*
     * Convert filters to SQL
     * 
     * Handles only simple syntax like "(institution:MyInst) AND (building:Main OR building:Branch)" 
     * or "(institution:MyInst) AND building:(Main OR Branch)"
     */
    private AbstractMap.SimpleEntry<String, List<String>> filterQueryToSQL(Query query)
        throws SQLException
    {
        if (query == null) {
            return null;
        }
        
        if (filterTypeMap == null) {
            PreparedStatement rowStmnt = db.prepareStatement ("select id, type from filter_type");
            ResultSet rs = rowStmnt.executeQuery ();
    
            filterTypeMap = new HashMap<String, Integer> ();
            while (rs.next ()) {
                filterTypeMap.put(rs.getString ("type"), rs.getInt ("id"));
            }
            rs.close ();
            rowStmnt.close ();
        }
        
        if (query instanceof BooleanQuery) {
            BooleanQuery bq = (BooleanQuery)query;
            String sql = "";
            List<String> parameters = new LinkedList<String> ();
            for (BooleanClause c: bq.getClauses ()) {
                AbstractMap.SimpleEntry<String, List<String>> res = filterQueryToSQL (c.getQuery ());
                if (res == null) {
                    continue;
                }
                
                if (!sql.isEmpty ()) {
                    if (c.isRequired ()) {
                        sql += " AND ";
                    } else if (c.isProhibited ()) {
                        sql += " AND NOT ";
                    } else {
                        sql += " OR ";
                    }
                } else {
                    if (c.isProhibited()) {
                        sql += " NOT ";
                    }
                }
                sql += "(" + res.getKey() + ")";
                parameters.addAll (res.getValue());
            }
            if (sql.isEmpty()) {
                return null;
            }
            return new AbstractMap.SimpleEntry<String, List<String>> (sql, parameters);
        } else if (query instanceof TermQuery) {
            TermQuery tq = (TermQuery)query;
            Term term = tq.getTerm ();
            
            if (!filterTypeMap.containsKey (term.field ())) {
                Log.info("BrowseRequestHandler: ignoring unknown filter field '" + term.field () + "'");
                return null;
            }
            String filterId = filterTypeMap.get (term.field ()).toString (); 

            String sql = "id in (select heading_id from filter_link where filter_value_id in (select id from filter_value where type_id=" + filterId + " and value=?))";
            List<String> parameters = new LinkedList<String> ();
            parameters.add(term.text ());
            return new AbstractMap.SimpleEntry<String, List<String>> (sql, parameters);
        } else if (query instanceof MatchAllDocsQuery) {
            return new AbstractMap.SimpleEntry<String, List<String>>("1=1", new LinkedList<String>());
        } else {
            Log.info("Unhandled Query class: " + query.getClass().getName ());
        }
        return null;
    }
    
    
    /*
     *
     * Sees that the browse index was rebuilt and opens the new database
     *
     */
    synchronized public void reopenIfUpdated () throws Exception
    {
        dbLock.readLock ().lock ();

        File flag = new File (path + "-ready");
        File updated = new File (path + "-updated");
        if (db == null || (flag.exists () && updated.exists ())) {
            Log.info ("Index update event detected!");
            try {
                dbLock.readLock ().unlock ();
                dbLock.writeLock ().lock ();

                if (flag.exists () && updated.exists ()) {
                    Log.info ("Installing new index version...");
                    if (db != null) {
                        db.close ();
                    }

                    File pathFile = new File (path);
                    pathFile.delete ();
                    updated.renameTo (pathFile);
                    flag.delete ();

                    Log.info ("Reopening HeadingsDB");
                    openDB ();
                } else if (db == null) {
                    openDB ();
                }
            } finally {
                dbLock.readLock ().lock ();
                dbLock.writeLock ().unlock ();
            }
        }
    }

    public void queryFinished ()
    {
        dbLock.readLock ().unlock ();
    }


    /*
     *
     * This function finds the starting row in the database when a string is passed
     * To the interface
     *
     * Parameters
     *    from      - the string to use to locate the heading
     *    filters   - Query filters
     *
     * Returns the rowid to start the list of headings
     *
     */
    public int getHeadingStart (String from, Query filters) throws Exception
    {
        int rowidResult;
        String[] tmp_build;
        String delimiter = ":";
        String sql_statement =  "select max(rowid) as id from headings where key < ?";

        AbstractMap.SimpleEntry<String, List<String>> filterList = filterQueryToSQL(filters);
        if (filterList != null) {
            sql_statement += " and (" + filterList.getKey() + ")";
        } 

        PreparedStatement rowStmnt = db.prepareStatement (sql_statement);

        rowStmnt.setBytes (1, normaliser.normalise (from));

        if (filterList != null) {
            int pos = 1;
            for (String value: filterList.getValue()) {
                rowStmnt.setString(++pos, value);
            }
        } 

        ResultSet rs = rowStmnt.executeQuery ();

        if (rs.next ()) {
            rowidResult = rs.getInt ("id");
        } else {
            rowidResult = totalCount + 1;   // past the end
        }

        return rowidResult;
    }


    /*
     *
     * This function retrieves the list of browse headings from the sqlite db
     *
     * Parameters
     *    rowid     - entry point to start the list
     *    rows      - number of entries to return
     *    filters   - Query filters
     *
     */
    public HeadingSlice getHeadings (int rowid,
                                     int rows,
                                     Query filters)
        throws Exception
    {
        HeadingSlice result = new HeadingSlice ();
        String[] tmp_build;
        String delimiter = ":";
        String sql_statement =  "select rowid, * from headings where rowid >= ?";
        int resultCounter = 0;
        int lastRowid = rowid;
        result.startRow = rowid;

        AbstractMap.SimpleEntry<String, List<String>> filterList = filterQueryToSQL(filters);
        if (filterList != null) {
            sql_statement += " and (" + filterList.getKey() + ")";
        } 
        
        sql_statement += " order by rowid limit " + Integer.toString(rows);

        PreparedStatement rowStmnt = db.prepareStatement (sql_statement);

        rowStmnt.setInt (1, rowid);

        if (filterList != null) {
            int pos = 1;
            for (String value: filterList.getValue()) {
                rowStmnt.setString(++pos, value);
            }
        } 
        
        ResultSet rs = null;

        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                rs = rowStmnt.executeQuery ();
                break;
            } catch (SQLException e) {
                Log.info ("Retry number " + attempt + "...");
                Thread.sleep (50);
            }
        }

        if (rs == null) {
            return result;
        }

        while (rs.next ()) {
            result.headings.add (rs.getString ("heading"));
            lastRowid = rs.getInt("rowid");
            resultCounter++;
        }

        rs.close ();
        rowStmnt.close ();

        result.total = resultCounter;
        result.endRow = lastRowid;

        return result;
    }

    /*
     *
     * This function retrieves the list of browse headings from the sqlite db
     *
     * Parameters
     *    rowid     - entry point to end the list
     *    rows      - number of entries to return
     *    filters   - Query filters
     *
     */
    public HeadingSlice getHeadingsPrevious (int rowid,
                                             int rows,
                                             Query filters)
        throws Exception
    {
        HeadingSlice result = new HeadingSlice ();
        String[] tmp_build;
        String delimiter = ":";
        String sql_statement =  "select rowid, * from headings where rowid <= ?";
        int resultCounter = 0;
        int lastRowid = rowid;
        result.endRow = rowid;

        AbstractMap.SimpleEntry<String, List<String>> filterList = filterQueryToSQL(filters);
        if (filterList != null) {
            sql_statement += " and (" + filterList.getKey() + ")";
        } 
        
        sql_statement += " order by rowid desc limit " + Integer.toString(rows);

        PreparedStatement rowStmnt = db.prepareStatement (sql_statement);

        rowStmnt.setInt (1, rowid);
        if (filterList != null) {
            int pos = 1;
            for (String value: filterList.getValue()) {
                rowStmnt.setString(++pos, value);
            }
        } 

        ResultSet rs = null;
        // Log.info ("totalCount is: " + totalCount + " and rowid is: " + rowid);

        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                rs = rowStmnt.executeQuery ();
                break;
            } catch (SQLException e) {
                Log.info ("Retry number " + attempt + "...");
                Thread.sleep (50);
            }
        }

        if (rs == null) {
            return result;
        }

        //  Need to add these in reverse order
        while (rs.next ()) {
            result.headings.addFirst (rs.getString ("heading"));
            lastRowid = rs.getInt("rowid");
            resultCounter++;
        }

        rs.close ();
        rowStmnt.close ();

        result.total = resultCounter;
        result.startRow = lastRowid;

        return result;
    }
}

/*
 *
 *  Interface to the Solr Lucene DB
 *
 */
class LuceneDB
{
    static Map<String,LuceneDB> dbs = new HashMap<String,LuceneDB> ();

    IndexSearcher searcher;
    String dbpath;
    long currentVersion = -1;


    public synchronized static LuceneDB getOrCreate (String path)
        throws Exception
    {
        if (!dbs.containsKey (path)) {
            LuceneDB db = new LuceneDB (path);
            dbs.put (path, db);
        }

        return dbs.get (path);
    }


    public synchronized static void reopenAllIfUpdated ()
        throws Exception
    {
        for (LuceneDB db : dbs.values ()) {
            db.reopenIfUpdated ();
        }
    }


    public LuceneDB (String path) throws Exception
    {
        this.dbpath = path;
    }


    private void openSearcher () throws Exception
    {
        if (searcher != null) {
            searcher.getIndexReader().close ();
        }

        IndexReader dbReader = DirectoryReader.open(FSDirectory.open (new File (dbpath)));
        searcher = new IndexSearcher (dbReader);
        currentVersion = indexVersion ();
    }


    public TopDocs search (Query q, int n) throws Exception
    {
        return searcher.search (q, n);
    }


    public TopDocs search (Query q, Filter fq, int n) throws Exception
    {
        return searcher.search (q, fq, n);
    }


    private long indexVersion ()
    {
        return new File (dbpath + "/segments.gen").lastModified ();
    }


    private boolean isDBUpdated ()
    {
        return (currentVersion != indexVersion ());
    }


    public Document getDocument (int docid) throws Exception
    {
        return searcher.getIndexReader ().document (docid);
    }


    public synchronized void reopenIfUpdated () throws Exception
    {
        if (isDBUpdated ()) {
            openSearcher ();
            Log.info ("Reopened " + searcher + " (" + dbpath + ")");
        }
    }
}



/*
 *
 * Interface to the Solr Authority DB
 *
 */
class AuthDB
{
    static int MAX_PREFERRED_HEADINGS = 1000;

    private LuceneDB db;
    private String preferredHeadingField;
    private String useInsteadHeadingField;
    private String seeAlsoHeadingField;
    private String scopeNoteField;

    public AuthDB (String path,
                   String preferredField,
                   String useInsteadField,
                   String seeAlsoField,
                   String noteField)
        throws Exception
    {
        db = LuceneDB.getOrCreate (path);
        preferredHeadingField = preferredField;
        useInsteadHeadingField = useInsteadField;
        seeAlsoHeadingField = seeAlsoField;
        scopeNoteField = noteField;
    }


    private List<String> docValues (Document doc, String field)
    {
        String values[] = doc.getValues (field);

        if (values == null) {
            values = new String[] {};
        }

        return Arrays.asList (values);
    }


    public void reopenIfUpdated () throws Exception
    {
        db.reopenIfUpdated ();
    }


    public Document getAuthorityRecord (String heading)
        throws Exception
    {
        TopDocs results = (db.search (new TermQuery (new Term (preferredHeadingField,
                                                               heading)),
                                      1));

        if (results.totalHits > 0) {
            return db.getDocument (results.scoreDocs[0].doc);
        } else {
            return null;
        }
    }


    public List<Document> getPreferredHeadings (String heading)
        throws Exception
    {
        TopDocs results = (db.search (new TermQuery (new Term (useInsteadHeadingField,
                                                               heading)),
                                      MAX_PREFERRED_HEADINGS));

        List<Document> result = new Vector<Document> ();

        for (int i = 0; i < results.totalHits; i++) {
            result.add (db.getDocument (results.scoreDocs[i].doc));
        }

        return result;
    }


    public Map<String, List<String>> getFields (String heading)
        throws Exception
    {
        Document authInfo = getAuthorityRecord (heading);

        Map<String, List<String>> itemValues =
            new HashMap<String,List<String>> ();

        itemValues.put ("seeAlso", new ArrayList<String>());
        itemValues.put ("useInstead", new ArrayList<String>());
        itemValues.put ("note", new ArrayList<String>());

        if (authInfo != null) {
            for (String value : docValues (authInfo, seeAlsoHeadingField)) {
                itemValues.get ("seeAlso").add (value);
            }

            for (String value : docValues (authInfo, scopeNoteField)) {
                itemValues.get ("note").add (value);
            }
        } else {
            List<Document> preferredHeadings =
                getPreferredHeadings (heading);

            for (Document doc : preferredHeadings) {
                for (String value : docValues (doc, preferredHeadingField)) {
                    itemValues.get ("useInstead").add (value);
                }
            }
        }

        return itemValues;
    }
}



/*
 *
 * Interface to the Solr biblio db
 *
 */
class BibDB
{
    private IndexSearcher db;
    private String field;
    private SolrParams params;
    private SolrQueryRequest request;

    public BibDB (IndexSearcher searcher, String field, SolrParams params, SolrQueryRequest request) throws Exception
    {
        db = searcher;
        this.field = field;
        this.params = params;
        this.request = request;
    }


    public int recordCount (String heading)
        throws Exception
    {
        TermQuery q = new TermQuery (new Term (field, heading));

        TotalHitCountCollector counter = new TotalHitCountCollector();
        db.search (q, counter);

        Log.info ("Hits: " + counter.getTotalHits ());
        
        return counter.getTotalHits ();
    }


    /*
     * Function to retrieve the doc ids with optional filters
     * This retrieves the doc ids for an individual heading
     */
    public Map<String, List<String>> matchingIDs (String heading, String extras, Query filters)
        throws Exception
    {
        Filter queryFilter = null;
        if (filters != null) {
            queryFilter = new QueryWrapperFilter(filters);
        } 
        Query q = new TermQuery (new Term (field, heading));

        final Map<String, List<String>> bibinfo = new HashMap<String,List<String>> ();
        bibinfo.put ("ids", new ArrayList<String> ());
        final String[] bibExtras = extras.split(":");
        for (int i = 0; i < bibExtras.length; i++) {
            bibinfo.put (bibExtras[i], new ArrayList<String> ());
        }

        db.search (q, queryFilter, new Collector () {
                private int docBase;

                public void setScorer (Scorer scorer) {
                }

                public boolean acceptsDocsOutOfOrder () {
                    return true;
                }

                public void collect (int docnum) {
                    int docid = docnum + docBase;
                    try {
                        Document doc = db.getIndexReader ().document (docid);

                        String[] vals = doc.getValues ("id");
                        bibinfo.get("ids").add (vals[0]);
                        // We only want 1
                        for (int i = 0; i < bibExtras.length; i++) {
                            vals = doc.getValues (bibExtras[i]);
                            if (vals.length > 0) {
                                bibinfo.get(bibExtras[i]).add (vals[0]);
                            }
                        }
                    } catch (org.apache.lucene.index.CorruptIndexException e) {
                        Log.info ("CORRUPT INDEX EXCEPTION.  EEK! - " + e);
                    } catch (Exception e) {
                        Log.info ("Exception thrown: " + e);
                    }

                }

                public void setNextReader (AtomicReaderContext context) {
                    this.docBase = context.docBase;
                }
            });

        return bibinfo;
    }
}



class BrowseList
{
    public int totalCount;
    public int startRow;
    public int endRow;
    public List<BrowseItem> items = new LinkedList<BrowseItem> ();


    public List<Map<String, Object>> asMap ()
    {
        List<Map<String, Object>> result = new LinkedList<Map<String, Object>> ();

        for (BrowseItem item : items) {
            result.add (item.asMap ());
        }

        return result;
    }
}



class BrowseItem
{
    public List<String> seeAlso = new LinkedList<String> ();
    public List<String> useInstead = new LinkedList<String> ();
    public String note = "";
    public String heading;
    public List<String> ids;
    public Map<String, List<String>> extras = new HashMap<String, List<String>> ();
    int count;


    public BrowseItem (String heading)
    {
        this.heading = heading;
    }

    
    public Map<String, Object> asMap ()
    {
        Map<String, Object> result = new HashMap<String, Object> ();

        result.put ("heading", heading);
        result.put ("seeAlso", seeAlso);
        result.put ("useInstead", useInstead);
        result.put ("note", note);
        result.put ("count", new Integer (count));
        result.put ("ids", ids);
        result.put ("extras", extras);

        return result;
    }
}




class Browse
{
    private HeadingsDB headingsDB;
    private AuthDB authDB;
    private BibDB bibDB;


    public Browse (HeadingsDB headings, AuthDB auth)
    {
        headingsDB = headings;
        authDB = auth;
    }


    public void setBibDB (BibDB b)
    {
        this.bibDB = b;
    }


    public synchronized void reopenDatabasesIfUpdated () throws Exception
    {
        headingsDB.reopenIfUpdated ();
        authDB.reopenIfUpdated ();
    }


    public void queryFinished ()
    {
        headingsDB.queryFinished ();
    }


    private void populateItem (BrowseItem item, String extras, Query filters) throws Exception
    {
        Map<String, List<String>> bibinfo = bibDB.matchingIDs (item.heading, extras, filters);
        item.ids = bibinfo.get("ids");
        bibinfo.remove("ids");
        item.count = item.ids.size ();

        // Need to go through the map and add
        item.extras = bibinfo;


        Map<String, List<String>> fields = authDB.getFields (item.heading);

        for (String value : fields.get ("seeAlso")) {
            if (bibDB.recordCount (value) > 0) {
                item.seeAlso.add (value);
            }
        }

        for (String value : fields.get ("useInstead")) {
            if (bibDB.recordCount (value) > 0) {
                item.useInstead.add (value);
            }
        }

        for (String value : fields.get ("note")) {
            item.note = value;
        }
    }

    /*
     *
     * getId
     *    This function finds the start of the browse list when given a string
     *
     */
    public int getId (String from, Query filters) throws Exception
    {
        return headingsDB.getHeadingStart (from, filters);
    }

    public BrowseList getList (int rowid, int offset, int rows, Query filters, String extras)
        throws Exception
    {
        BrowseList result = new BrowseList ();
        HeadingSlice h = new HeadingSlice ();

        if (offset < 0) {
            h = headingsDB.getHeadingsPrevious (Math.max (0, rowid), rows, filters);
        } else {
            h = headingsDB.getHeadings (Math.max (0, rowid), rows, filters);
        }

        result.totalCount = h.total;
        result.startRow = h.startRow;
        result.endRow = h.endRow;

        for (String heading : h.headings) {
            BrowseItem item = new BrowseItem (heading);
            populateItem (item, extras, filters);
            result.items.add (item);
        }

        return result;
    }
}



class BrowseSource
{
    public String DBpath;
    public String field;
    public String dropChars;

    public Browse browse;


    public BrowseSource (String DBpath,
                         String field,
                         String dropChars)
    {
        this.DBpath = DBpath;
        this.field = field;
        this.dropChars = dropChars;
    }
}



public class BrowseRequestHandler extends RequestHandlerBase
{
    private String authPath = null;
    private String bibPath = null;

    private Map<String,BrowseSource> sources = new HashMap<String,BrowseSource> ();

    private SolrParams solrParams;


    private String asAbsFile (String s)
    {
        File f = new File (s);

        if (!f.isAbsolute ()) {
            return (new File (new File (System.getenv ("BROWSE_HOME")),
                             f.getPath ()).getPath ());
        } else {
            return f.getPath ();
        }
    }


    public void init (NamedList args)
    {
        super.init (args);

        solrParams = SolrParams.toSolrParams (args);

        authPath = asAbsFile (solrParams.get ("authIndexPath"));
        bibPath = asAbsFile (solrParams.get ("bibIndexPath"));

        sources = new HashMap<String, BrowseSource> ();

        for (String source : Arrays.asList (solrParams.get
                                            ("sources").split (","))) {
            @SuppressWarnings("unchecked")
            NamedList<String> entry = (NamedList<String>)args.get (source);

            sources.put (source,
                         new BrowseSource (entry.get ("DBpath"),
                                           entry.get ("field"),
                                           entry.get ("dropChars")));
        }
    }


    private int asInt (String s)
    {
        int value;
        try {
            return new Integer (s).intValue ();
        } catch (NumberFormatException e) {
            return 0;
        }
    }
   
    /*
     *
     * The main body that receives the parameters and returns the results
     * Possible parameters
     *   from     - string to start the browse
     *   filters  - list of filters to use as limit
     *                   when searching the sqlite database
     *   offset   - If -1 browse backward, if 0 browse forward
     *   rows     - number of entries to return in result set
     *   rowid    - rowid of sqlite db to use as starting point for entries
     *                  from should be empty when this is populated
     *   source   - sqlite database to query
     *   json.nl  - ???? - from previous code
     *   wt       - ???? - from previous code
     *
    */

    @Override
    public void handleRequestBody (org.apache.solr.request.SolrQueryRequest req,
                                   org.apache.solr.response.SolrQueryResponse rsp)
        throws Exception
    {
        SolrParams p = req.getParams ();

        if (p.get ("reopen") != null) {
            LuceneDB.reopenAllIfUpdated ();
            return;
        }


        String sourceName = p.get ("source");
        String from = p.get ("from");
        String filters = p.get ("filters");
        String extras = p.get ("extras");

        // extras needs to be a non-null string
        if (extras == null) {
            extras = "";
        }

        int rowid = -1;
        if (p.get ("rowid") != null && !p.get ("rowid").equals ("")) {
            rowid = asInt (p.get ("rowid"));
        }

        int rows = asInt (p.get ("rows"));

        int offset = (p.get ("offset") != null) ? asInt (p.get ("offset")) : 0;

        if (rows < 0) {
            throw new Exception ("Invalid value for parameter: rows");
        }

        if (sourceName == null || !sources.containsKey (sourceName)) {
            throw new Exception ("Need a (valid) source parameter.");
        }

        
        BrowseSource source = sources.get (sourceName);

        Query filterQuery = null;
        if (filters != null && !filters.isEmpty()) {
            // Prepend with a *:* query so that any negative filter queries work
            filters = "*:* " + filters;
            QParser analyzer = new LuceneQParserPlugin().createParser(filters, p, p, req); 
            QueryParser queryParser = new QueryParser(Version.LUCENE_43, "allfields", analyzer);
            filterQuery = queryParser.parse(filters);
        }
        
        synchronized (this) {
            if (source.browse == null) {
                source.browse = (new Browse
                                 (new HeadingsDB (source.DBpath),
                                  new AuthDB
                                  (authPath,
                                   solrParams.get ("preferredHeadingField"),
                                   solrParams.get ("useInsteadHeadingField"),
                                   solrParams.get ("seeAlsoHeadingField"),
                                   solrParams.get ("scopeNoteField"))));
            }

            source.browse.setBibDB (new BibDB (req.getSearcher (),
                                               source.field,
                                               p, req));
        }

        try {
            source.browse.reopenDatabasesIfUpdated ();

            if (from != null && rowid == -1) {
                rowid = (source.browse.getId (from, filterQuery));
            }

            BrowseList list = source.browse.getList (rowid, offset, rows, filterQuery, extras);

            Map<String,Object> result = new HashMap<String, Object> ();

            result.put ("totalCount", list.totalCount);
            result.put ("items", list.asMap ());
            result.put ("startRow", list.startRow);
            result.put ("endRow", list.endRow);
            result.put ("offset", offset);

            rsp.add ("Browse", result);
        } finally {
            source.browse.queryFinished ();
        }
    }


    //////////////////////// SolrInfoMBeans methods //////////////////////

    public String getVersion () {
        return "$Revision: 0.1 $";
    }

    public String getDescription () {
        return "NLA browse handler";
    }

    public String getSourceId () {
        return "";
    }

    public String getSource () {
        return "";
    }

    public URL[] getDocs () {
        return null;
    }
}
