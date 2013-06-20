package au.gov.nla.util;

public class Utils
{
    public static String getEnvironment (String var)
    {
        return (System.getenv (var) != null) ?
            System.getenv (var) : System.getProperty (var.toLowerCase ());
    }
    
    public static String trimTerm (String term)
    {
        if (term.length() <= 255) {
            return term;
        }
        // Cut from next space
        int pos = term.indexOf(' ', 255);
        if (pos > 0) {
            return term.substring(0, pos);
        }
        return term;
    }
}
