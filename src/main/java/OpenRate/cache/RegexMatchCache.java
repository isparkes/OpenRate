/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2015.
 *
 * The following restrictions apply unless they are expressly relaxed in a
 * contractual agreement between the license holder or one of its officially
 * assigned agents and you or your organisation:
 *
 * 1) This work may not be disclosed, either in full or in part, in any form
 *    electronic or physical, to any third party. This includes both in the
 *    form of source code and compiled modules.
 * 2) This work contains trade secrets in the form of architecture, algorithms
 *    methods and technologies. These trade secrets may not be disclosed to
 *    third parties in any form, either directly or in summary or paraphrased
 *    form, nor may these trade secrets be used to construct products of a
 *    similar or competing nature either by you or third parties.
 * 3) This work may not be included in full or in part in any application.
 * 4) You may not remove or alter any proprietary legends or notices contained
 *    in or on this work.
 * 5) This software may not be reverse-engineered or otherwise decompiled, if
 *    you received this work in a compiled form.
 * 6) This work is licensed, not sold. Possession of this software does not
 *    imply or grant any right to you.
 * 7) You agree to disclose any changes to this work to the copyright holder
 *    and that the copyright holder may include any such changes at its own
 *    discretion into the work
 * 8) You agree not to derive other works from the trade secrets in this work,
 *    and that any such derivation may make you liable to pay damages to the
 *    copyright holder
 * 9) You agree to use this software exclusively for evaluation purposes, and
 *    that you shall not use this software to derive commercial profit or
 *    support your business or personal activities.
 *
 * This software is provided "as is" and any expressed or impled warranties,
 * including, but not limited to, the impled warranties of merchantability
 * and fitness for a particular purpose are disclaimed. In no event shall
 * The OpenRate Project or its officially assigned agents be liable to any
 * direct, indirect, incidental, special, exemplary, or consequential damages
 * (including but not limited to, procurement of substitute goods or services;
 * Loss of use, data, or profits; or any business interruption) however caused
 * and on theory of liability, whether in contract, strict liability, or tort
 * (including negligence or otherwise) arising in any way out of the use of
 * this software, even if advised of the possibility of such damage.
 * This software contains portions by The Apache Software Foundation, Robert
 * Half International.
 * ====================================================================
 */
package OpenRate.cache;

import OpenRate.OpenRate;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.logging.LogUtil;
import OpenRate.utils.PropertyUtils;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;


/**
 * Please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Regex_Match_Cache'>click here</a> to go to wiki page.
 * <br>
 * <p>
 * This class implements an IRule equivalent that can evaluate a defined regular
 * expression pattern. For simplicity, the number of columns in the match
 * pattern will be read from the first row of the data that is read from the
 * input source. Thereafter, all rows must have the same form, which will be
 * defined as the form factor of the data.
 */
public class RegexMatchCache
     extends AbstractSyncLoaderCache
{
  /**
   * The default return when there is no match
   */
  public static final String NO_REGEX_MATCH = "NOMATCH";

  // this is the form factor of the data (the number of columns to read
  private int KeyFormFactor = 0;

  // List of Services that this Client supports
  private final static String SERVICE_OBJECT_COUNT = "ObjectCount";
  private final static String SERVICE_GROUP_COUNT = "GroupCount";
  private final static String SERVICE_DUMP_MAP = "DumpMap";

 /* The SearchMap is the regular map that we will have to search through. This 
  * is a single entry that is grouped into a search group. The match value is
  * one of [RegularExpression|Numerical|RegexExclude], driven by the match type.
  * 
  * The 
  */
  private class SearchMap
  {
    // Depending on the type, we do a real regex, or a comparison
    // 0 = regex
    // 1 = "="
    // 2 = ">"
    // 3 = "<"
    // 4 = ">="
    // 5 = "<="
    // 6 = regex EXCLUDE
    int[]     matchType;

    // We can match this if the type > 0
    double[]  matchValue;

    // Or this if we are dealing with a real regex
    Pattern[] matchPattern;
    
    // The results list
    ArrayList<String> Results = null;
  }

  /* The SearchGroup is the collection of search maps that will be searched
   * during the evaluation.
   */
  private class SearchGroup
  {
    ArrayList<SearchMap> SearchGroup;
  }

 /**
  * The internal cache is organised as a hash of the regex map groups that
  * have been defined, each of which holds a variable number of entries to
  * search through. This is therefore the index to the group entries.
  */
  private HashMap<String, SearchGroup> GroupCache;

 /** Constructor
  * Creates a new instance of the Regex Map Group Cache. The Cache contains
  * all of the groups that are known to the module. We estimate the size of
  * the hash to some value that is reasonable.
  */
  public RegexMatchCache()
  {
    super();

    GroupCache = new HashMap<>(200);
  }

// -----------------------------------------------------------------------------
// ----------------- Start of overridable Plug In functions --------------------
// -----------------------------------------------------------------------------
 /**
  * The method allows the implementation class the possibility to manipulate
  * or validate the search map fields before they are stored
  *
  * @param inputSearchMap The search map to validate or manipulate
   * @return the modified or checked search map
   * @throws InitializationException
  */
  public String[] validateSearchMap(String[] inputSearchMap) throws InitializationException
  {
    // Pass through - override to change
    return inputSearchMap;
  }

 /**
  * The method allows the implementation class the possibility to validate or
  * manipulate the array list return list
  *
  * @param inputResultList The result list to validate or manipulate
   * @return the modified or checked result list
   * @throws InitializationException
  */
  public ArrayList<String> validateResultList(ArrayList<String> inputResultList) throws InitializationException
  {
    // Pass through - override to change
    return inputResultList;
  }

// -----------------------------------------------------------------------------
// ------------------ Start of inherited Plug In functions ---------------------
// -----------------------------------------------------------------------------

 /**
  * loadCache is called automatically on startup of the
  * cache factory, as a result of implementing the CacheLoader
  * interface. This should be used to load any data that needs loading, and
  * to set up variables.
  *
  * @throws InitializationException
  */
  @Override
  public void loadCache(String ResourceName, String CacheName)
                 throws InitializationException
  {
    // Variable definitions
    String tmpValue;

    // Get the number of key fields (the rest are treated as results)
    tmpValue = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                     CacheName,
                                                     "KeyFields",
                                                     "None");
    if (tmpValue.equals("None"))
    {
      // We will use calculate the default assuming 1 result field
      KeyFormFactor = 0;
    }
    else
    {
      try
      {
        KeyFormFactor = Integer.parseInt(tmpValue);
      }
      catch(NumberFormatException nfe)
      {
        message = "KeyFields entry for cache <" + getSymbolicName() +
                          "> not numeric. Found value <" + tmpValue + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message,getSymbolicName());
      }
    }

    // Now perform the base initialisation
    super.loadCache(ResourceName, CacheName);
 }

 /**
  * Add a value into the Regex Map Cache, defining the result value that
  * should be returned in the case of a match. The order of evaluation of the
  * items in the group is the order that they are defined, but it would be a
  * simple task to order them by some value after loading.
  *
  * @param Group The Regex group to add this pattern to
  * @param fields The list of fields to add to the group
  * @param ResultList The list of result fields to add
  * @throws OpenRate.exception.InitializationException
  */
  private void addEntry(String Group, String[] fields, ArrayList<String> ResultList)
    throws InitializationException
  {
    int         i;
    SearchMap   tmpSearchMap;
    SearchGroup tmpSearchGroup;
    String      Helper;
    String      FirstChar;
    String      SecondChar;
    String      valueToParse;
    String[]    checkedFields;
    ArrayList<String> checkedResultList;

    // Allow the user to check the input fields
    checkedFields = validateSearchMap(fields);

    // Allow the user to check the return fields
    checkedResultList = validateResultList(ResultList);

    // See if we already know this group, if not add it
    if (GroupCache.containsKey(Group))
    {
      // Get the existing value
      tmpSearchGroup = GroupCache.get(Group);
    }
    else
    {
      // We don't know it, so add it
      tmpSearchGroup = new SearchGroup();
      tmpSearchGroup.SearchGroup = new ArrayList<>();
      GroupCache.put(Group, tmpSearchGroup);
    }

    // Create the new search Object.
    tmpSearchMap = new SearchMap();

    // Compile and add the search map
    tmpSearchMap.matchPattern = new Pattern[checkedFields.length];
    tmpSearchMap.matchType = new int[checkedFields.length];
    tmpSearchMap.matchValue = new double[checkedFields.length];

    for (i = 0; i < fields.length; i++)
    {
      // get the short version of the string for understanding what it is
      Helper = fields[i].replaceAll(" ","") + "  ";

      FirstChar = Helper.substring(0,1);
      SecondChar = Helper.substring(1,2);

      if ((FirstChar.equals("<")) |
          (FirstChar.equals(">")) |
          (FirstChar.equals("=")))
      {
        // try to parse for simple numerical comparison
        if (FirstChar.equals("="))
        {
          tmpSearchMap.matchType[i] = 1;
          valueToParse = Helper.substring(1).trim();
          
          try
          {
            tmpSearchMap.matchValue[i] = Double.parseDouble(valueToParse);
          }
          catch (NumberFormatException ex)
          {
            throw new InitializationException("Could not parse value <" + valueToParse + "> as a double value",getSymbolicName());
          }
          
          continue;
        }

        if (FirstChar.equals(">"))
        {
          if (SecondChar.equals("="))
          {
            tmpSearchMap.matchType[i] = 4;
            valueToParse = Helper.substring(2).trim();
            try
            {
              tmpSearchMap.matchValue[i] = Double.parseDouble(valueToParse);
            }
            catch (NumberFormatException ex)
            {
              throw new InitializationException("Could not parse value <" + valueToParse + "> as a double value",getSymbolicName());
            }
            
            continue;
          }
          else
          {
            // we got this far, must be just ">"
            tmpSearchMap.matchType[i] = 2;
            valueToParse = Helper.substring(1).trim();
            
            try
            {
              tmpSearchMap.matchValue[i] = Double.parseDouble(valueToParse);
            }
            catch (NumberFormatException ex)
            {
              throw new InitializationException("Could not parse value <" + valueToParse + "> as a double value",getSymbolicName());
            }
            
            continue;
          }
        }

        if (FirstChar.equals("<"))
        {
          if (SecondChar.equals("="))
          {
            tmpSearchMap.matchType[i] = 5;
            valueToParse = Helper.substring(2).trim();
            
            try
            {
              tmpSearchMap.matchValue[i] = Double.parseDouble(valueToParse);
            }
            catch (NumberFormatException ex)
            {
              throw new InitializationException("Could not parse value <" + valueToParse + "> as a double value",getSymbolicName());
            }
            
            continue;
          }
          else
          {
            // we got this far, must be just "<"
            tmpSearchMap.matchType[i] = 3;
            valueToParse = Helper.substring(1).trim();
            
            try
            {
              tmpSearchMap.matchValue[i] = Double.parseDouble(valueToParse);
            }
            catch (NumberFormatException ex)
            {
              throw new InitializationException("Could not parse value <" + valueToParse + "> as a double value",getSymbolicName());
            }

            continue;
          }
        }
      }
      else
      {
        if(FirstChar.equals("!"))
        {
          // This is a regex negation, remove the ! and set the flag and regex
          // for the rest
          tmpSearchMap.matchPattern[i] = Pattern.compile(fields[i].substring(1));
          tmpSearchMap.matchType[i] = 6;
        }
        else
        {
          // if we got this far it is Real Regex inclusion
          try
          {
            tmpSearchMap.matchPattern[i] = Pattern.compile(fields[i]);
          }
          catch (PatternSyntaxException pse)
          {
            message = "Error compiling regex pattern <" + fields[i] +
                      "> in module <" + getSymbolicName() + ">. message <" + pse.getMessage() + ">";
            OpenRate.getOpenRateFrameworkLog().error(message);
            throw new InitializationException(message,getSymbolicName());
          }

          tmpSearchMap.matchType[i] = 0;
        }
      }
    }

    tmpSearchMap.Results = checkedResultList;
    tmpSearchGroup.SearchGroup.add(tmpSearchMap);
  }

  /**
   * Evaluate an input against the search group. This is the generalised from
   * which you may want to create specialised versions for a defined number of
   * parameters, for reasons of performance.
   *
   * This function returns only the first match. If you want to get all matching
   * entries, this is done using getAllEntries()
   *
  * @param Group The Regex group to search
  * @param Parameters The list of fields to search
  * @return Result The result of the search
   */
  public String getMatch(String Group, String[] Parameters)
  {
    SearchMap tmpSearchResult;

    tmpSearchResult = getMatchingSearchResult(Group,Parameters);

    if (tmpSearchResult == null)
    {
      return NO_REGEX_MATCH;
    }
    else
    {
      return tmpSearchResult.Results.get(0);
    }
  }

  /**
   * Evaluate an input against the search group. This is the generalised from
   * which you may want to create specialised versions for a defined number of
   * parameters, for reasons of performance.
   *
   * This function returns only the first match. If you want to get all matching
   * entries, this is done using getAllEntries()
   *
  * @param Group The Regex group to search
  * @param Parameters The list of fields to search
  * @return Result The result of the search as a vector of strings
   */
  public ArrayList<String> getMatchWithChildData(String Group, String[] Parameters)
  {
    SearchMap tmpSearchResult;
    ArrayList<String> tmpResult;

    tmpSearchResult = getMatchingSearchResult(Group,Parameters);

    if (tmpSearchResult == null)
    {
      tmpResult = new ArrayList<>();
      tmpResult.add(NO_REGEX_MATCH);

      return tmpResult;
    }
    else
    {
      return tmpSearchResult.Results;
    }
  }

  /**
   * Evaluate an input against the search group. This is the generalised from
   * which you may want to create specialised versions for a defined number of
   * parameters, for reasons of performance.
   *
   * This function returns only the first match.
   *
  * @param Group The Regular expression group to search
  * @param Parameters The list of fields to search
  * @return Result The result of the search as a SearchMap object
   */
  private SearchMap getMatchingSearchResult(String Group, String[] Parameters)
  {
    int         i;
    SearchGroup tmpSearchGroup;
    SearchMap   tmpSearchMap;
    Pattern     tmpPattern;
    boolean     Found;
    double      tmpParamValue;

    // recover the object
    tmpSearchGroup = GroupCache.get(Group);

    if (tmpSearchGroup == null)
    {
      // Return a default value
      return null;
    }
    else
    {
      // Iterate thorough the entries in the group
      Iterator<SearchMap> GroupIter = tmpSearchGroup.SearchGroup.listIterator();

      while (GroupIter.hasNext())
      {
        tmpSearchMap = GroupIter.next();

        // Initialise the found flag and the counter
        Found = true;
        i = 0;

        // Now check the elements of the map
        while ((i < Parameters.length) & Found)
        {
          switch(tmpSearchMap.matchType[i])
          {
            // Regex inclusion case
            case 0:
            {
              tmpPattern = tmpSearchMap.matchPattern[i];

              if (Parameters[i] == null)
              {
                // we cannot match on null values - warn once and out...
                OpenRate.getOpenRateFrameworkLog().warning("Null value found in regex match on parameter <" + i + "> in module <" + getSymbolicName() + ">");
                return null;
              }

              if (!tmpPattern.matcher(Parameters[i]).matches())
              {
                // We did not get a match, move on
                Found = false;
              }
              break;
            }

            // Regex exclusion case
            case 6:
            {
              tmpPattern = tmpSearchMap.matchPattern[i];

              if (tmpPattern.matcher(Parameters[i]).matches())
              {
                // We did not get a match, move on
                Found = false;
              }
              break;
            }

            // "=" case
            case 1:
            {
              tmpParamValue = Double.parseDouble(Parameters[i]);
              if (tmpSearchMap.matchValue[i] != tmpParamValue)
              {
                // We did not get a match, move on
                Found = false;
              }
              break;
            }

            // ">" case
            case 2:
            {
              tmpParamValue = Double.parseDouble(Parameters[i]);
              if (tmpParamValue <= tmpSearchMap.matchValue[i])
              {
                // We did not get a match, move on
                Found = false;
              }
              break;
            }

            // "<" case
            case 3:
            {
              tmpParamValue = Double.parseDouble(Parameters[i]);
              if (tmpParamValue >= tmpSearchMap.matchValue[i])
              {
                // We did not get a match, move on
                Found = false;
              }
              break;
            }

            // ">=" case
            case 4:
            {
              tmpParamValue = Double.parseDouble(Parameters[i]);
              if (tmpParamValue < tmpSearchMap.matchValue[i])
              {
                // We did not get a match, move on
                Found = false;
              }
              break;
            }

            // "<=" case
            case 5:
            {
              tmpParamValue = Double.parseDouble(Parameters[i]);
              if (tmpParamValue > tmpSearchMap.matchValue[i])
              {
                // We did not get a match, move on
                Found = false;
              }
              break;
            }
          }

          // Increment the loop counter
          i++;
        }

        if (Found)
        {
          return tmpSearchMap;
        }
      }

      // Return a default value - we found nothing
      return null;
    }
  }

 /**
  * Evaluate an input against the search group. This is the generalised from
  * which you may want to create specialised versions for a defined number of
  * parameters, for reasons of performance.
  *
  * This function returns all of the entries that are matched, in priority
  * order. This is useful for aggregation processing etc.
  *
  * @param Group The Regex group to search
  * @param Parameters The list of fields to search
  * @return List of all matches
  */
  public ArrayList<String> getAllEntries(String Group, String[] Parameters)
  {
    int         i;
    SearchGroup tmpSearchGroup;
    SearchMap   tmpSearchMap;
    Pattern     tmpPattern;
    boolean     found;
    double      tmpParamValue;
    ArrayList<String> matches;

    matches = new ArrayList<>();

    // recover the object
    tmpSearchGroup = GroupCache.get(Group);

    if (tmpSearchGroup == null)
    {
      // Return a default value, we did not find the group
      return matches;
    }
    else
    {
      // Iterate thorough the entries in the group
      Iterator<SearchMap> GroupIter = tmpSearchGroup.SearchGroup.listIterator();

      while (GroupIter.hasNext())
      {
        tmpSearchMap = GroupIter.next();

        // Initialise the found flag and the counter
        found = true;
        i = 0;

        // Now check the elements of the map
        while ((i < Parameters.length) & found)
        {
          switch(tmpSearchMap.matchType[i])
          {
            // Regex inclusion case
            case 0:
            {
              tmpPattern = tmpSearchMap.matchPattern[i];

              if (!tmpPattern.matcher(Parameters[i]).matches())
              {
                // We did not get a match, move on
                found = false;
              }
              break;
            }

            // Regex exclusion case
            case 6:
            {
              tmpPattern = tmpSearchMap.matchPattern[i];

              if (tmpPattern.matcher(Parameters[i]).matches())
              {
                // We did not get a match, move on
                found = false;
              }
              break;
            }

            // "=" case
            case 1:
            {
              tmpParamValue = Double.parseDouble(Parameters[i]);
              if (tmpSearchMap.matchValue[i] != tmpParamValue)
              {
                // We did not get a match, move on
                found = false;
              }
              break;
            }

            // ">" case
            case 2:
            {
              tmpParamValue = Double.parseDouble(Parameters[i]);
              if (tmpParamValue <= tmpSearchMap.matchValue[i])
              {
                // We did not get a match, move on
                found = false;
              }
              break;
            }

            // "<" case
            case 3:
            {
              tmpParamValue = Double.parseDouble(Parameters[i]);
              if (tmpParamValue >= tmpSearchMap.matchValue[i])
              {
                // We did not get a match, move on
                found = false;
              }
              break;
            }

            // ">=" case
            case 4:
            {
              tmpParamValue = Double.parseDouble(Parameters[i]);
              if (tmpParamValue < tmpSearchMap.matchValue[i])
              {
                // We did not get a match, move on
                found = false;
              }
              break;
            }

            // "<=" case
            case 5:
            {
              tmpParamValue = Double.parseDouble(Parameters[i]);
              if (tmpParamValue > tmpSearchMap.matchValue[i])
              {
                // We did not get a match, move on
                found = false;
              }
              break;
            }
          }

          // Increment the loop counter
          i++;
        }

        if (found)
        {
          matches.add(tmpSearchMap.Results.get(0));
        }
      }

      return matches;
    }
  }

 /**
  * Load the data from the defined file
  */
  @Override
  public void loadDataFromFile()
    throws InitializationException
  {
    // Variable declarations
    BufferedReader    inFile;
    int               MapsLoaded = 0;
    String[]          MapEntryFields;
    String[]          SearchMapFields;
    String            tmpFileRecord;
    String            Group;
    int               ColumnCount;
    int               ColumnIdx;
    int               ResultFormFactor = 0;
    ArrayList<String> tmpResultList;

    // Find the location of the configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Regex Match Cache Loading from file for <" + getSymbolicName() + ">");

    // Try to open the file
    try
    {
      inFile = new BufferedReader(new FileReader(cacheDataFile));
    }
    catch (FileNotFoundException ex)
    {
      message = "Application is not able to read file : <" +
            cacheDataFile + ">";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // File open, now get the stuff
    try
    {
      while (inFile.ready())
      {
        tmpFileRecord = inFile.readLine();

        if ((tmpFileRecord.startsWith("#")) |
            tmpFileRecord.trim().equals(""))
        {
          // comment do not load
        }
        else
        {
          MapsLoaded++;
          MapEntryFields = tmpFileRecord.split(";");

          ColumnCount = MapEntryFields.length;

          // Read the form factor from the first entry, and then make sure
          // that we always get this
          if (ResultFormFactor == 0)
          {
            // Check that this is a form factor we are able to use. There
            // must be at least 3 fields for us to be able to work with
            // the record:
            //  - Group identifier
            //  - Some fields to compare (min 1)
            //  - a result
            if (ColumnCount < 3)
            {
              // we're not going to be able to use this
              message = "You must define at least 3 entries in the record, you have defined  <" +
                    ColumnCount + "> in module <"+getSymbolicName()+">";
              OpenRate.getOpenRateFrameworkLog().fatal(message);
              throw new InitializationException(message,getSymbolicName());
            }

            // Do we have a defined form factor?
            if (KeyFormFactor == 0)
            {
              // If we have not been given a key form factor, assume the result factor is 1
              ResultFormFactor = 1;
              KeyFormFactor = ColumnCount - ResultFormFactor - 1;
              message = "Using default key factor for regex cache <" +
                               cacheDataFile + ">. Assuming Key = <" +
                               Integer.toString(KeyFormFactor) + "> and Result = <" +
                               Integer.toString(ResultFormFactor) + 
                               "> in module <"+getSymbolicName()+">";
              OpenRate.getOpenRateFrameworkLog().info(message);
            }
            else
            {
              // Use the given factor
              ResultFormFactor = ColumnCount - KeyFormFactor - 1;
              message = "Using defined key factor for regex cache <" +
                               cacheDataFile + ">. Using Key = <" +
                               Integer.toString(KeyFormFactor) + "> and Result = <" +
                               Integer.toString(ResultFormFactor) + "> in module <"+ 
                               getSymbolicName()+">";
              OpenRate.getOpenRateFrameworkLog().info(message);
            }
          }

          // Check that we are always getting the same form factor
          if (MapEntryFields.length == (KeyFormFactor + ResultFormFactor + 1))
          {
            //Add the row, after extracting the group and the result
            Group = MapEntryFields[0];
            SearchMapFields = new String[KeyFormFactor];

            for (ColumnIdx = 0; ColumnIdx < KeyFormFactor; ColumnIdx++)
            {
              SearchMapFields[ColumnIdx] = MapEntryFields[ColumnIdx + 1];
            }

            tmpResultList = new ArrayList<>();
            for (ColumnIdx = KeyFormFactor; ColumnIdx < (ResultFormFactor + KeyFormFactor); ColumnIdx++)
            {
              tmpResultList.add(MapEntryFields[ColumnIdx + 1]);
            }

            // Add the map
            addEntry(Group, SearchMapFields, tmpResultList);
          }
          else
          {
            // Error because the form factor changed
            message = "Form Factor should be Key = <" + Integer.toString(KeyFormFactor) +
                  "> + Payload = <" + Integer.toString(ResultFormFactor) +
                  ">, but received <" + MapEntryFields.length + "> in module <"+getSymbolicName()+">";
            OpenRate.getOpenRateFrameworkLog().error(message);
            throw new InitializationException(message,getSymbolicName());
          }

          // Update to the log file
          if ((MapsLoaded % loadingLogNotificationStep) == 0)
          {
            message = "Regex Map Data Loading: <" + MapsLoaded +
                  "> configurations loaded for <" + getSymbolicName() + "> from <" +
                  cacheDataFile + ">";
            OpenRate.getOpenRateFrameworkLog().info(message);
          }
        }
      }
    }
    catch (IOException ex)
    {
      message = "Error reading input file  <" + cacheDataFile +
            "> in record <" + MapsLoaded + ">. IO Error.";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
    }
    catch (ArrayIndexOutOfBoundsException ex)
    {
      message = "Error reading input file  <" + cacheDataFile +
            "> in record <" + MapsLoaded + ">. Malformed Record.";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
    }
    catch (NullPointerException npe)
    {
      message = "Error reading input file  <" + cacheDataFile +
            "> in record <" + MapsLoaded + ">. Malformed Record in module <"+getSymbolicName()+">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
    }
    finally
    {
      try
      {
        inFile.close();
      }
      catch (IOException ex)
      {
        message = "Error closing input file <" + cacheDataFile + 
                         "> in module <"+getSymbolicName()+">";
        OpenRate.getOpenRateFrameworkLog().error(message, ex);
      }
    }

    message = "Regex Map Data Loading completed. <" + MapsLoaded +
          "> configuration lines loaded for <" + getSymbolicName() + "> from <" +
          cacheDataFile + ">";
    OpenRate.getOpenRateFrameworkLog().info(message);
  }

  /**
  * Load the data from the defined Data Source
  */
  @Override
  public void loadDataFromDB()
    throws InitializationException
  {
    int               ColumnIdx;
    int               ColumnCount;
    int               ConfigsLoaded = 0;
    String            Group;
    ResultSetMetaData Rsmd;
    String[]          SearchMapFields;
    int               ResultFormFactor = 0;
    ArrayList<String> tmpResultList;

    // Find the location of the  zone configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Regex Match Cache Loading from DB for <" + getSymbolicName() + ">");

    // Try to open the DS
    JDBCcon = DBUtil.getConnection(cacheDataSourceName);

    // Now prepare the statements
    prepareStatements();

    // Execute the query
    try
    {
      mrs = StmtCacheDataSelectQuery.executeQuery();
    }
    catch (SQLException ex)
    {
      message = "Error performing SQL for retrieving Regex Match data in module <"+getSymbolicName()+">. message <" + ex.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // loop through the results for the customer login cache
    try
    {
      Rsmd = mrs.getMetaData();
      ColumnCount = Rsmd.getColumnCount();

      // Read the form factor from the first entry, and then make sure
      // that we always get this
      if (ResultFormFactor == 0)
      {
        // Check that this is a form factor we are able to use. There
        // must be at least 3 fields for us to be able to work with
        // the record:
        //  - Group identifier
        //  - Some fields to compare (min 1)
        //  - a result
        if (ColumnCount < 3)
        {
          // we're not going to be able to use this
          message = "You must define at least 3 entries in the record, you have defined  <" +
                ColumnCount + ">";
          OpenRate.getOpenRateFrameworkLog().fatal(message);
          throw new InitializationException(message,getSymbolicName());
        }

        // Do we have a defined form factor?
        if (KeyFormFactor == 0)
        {
          // If we have not been given a key form factor, assume the result factor is 1
          ResultFormFactor = 1;
          KeyFormFactor = ColumnCount - ResultFormFactor - 1;
          message = "Using default key factor for regex cache <" +
                           cacheDataSourceName + ">. Assuming Key = <" +
                           Integer.toString(KeyFormFactor) + "> and Result = <" +
                           Integer.toString(ResultFormFactor) + ">";
          OpenRate.getOpenRateFrameworkLog().info(message);
        }
        else
        {
          // Use the given factor
          ResultFormFactor = ColumnCount - KeyFormFactor - 1;

          if (ResultFormFactor < 1)
          {
            // Makes no sense to start if we don't give any results
            message = "Error in module <" +
                  cacheDataSourceName + ">. Key fields >= total columns. Got KeyFields <" +
                    KeyFormFactor + "> and columns <" + ColumnCount + ">";
            OpenRate.getOpenRateFrameworkLog().fatal(message);
            throw new InitializationException(message,getSymbolicName());
          }

          message = "Using defined key factor for regex cache <" +
                           getSymbolicName() + ">. Using Key = <" +
                           Integer.toString(KeyFormFactor) + "> and Result = <" +
                           Integer.toString(ResultFormFactor) + ">";
          OpenRate.getOpenRateFrameworkLog().info(message);
        }
      }

      // Start the loading
      mrs.beforeFirst();
      while (mrs.next())
      {
        ConfigsLoaded++;
        Group = mrs.getString(1);

        SearchMapFields = new String[KeyFormFactor];

        // create the array to transfer the columns into the map
        for (ColumnIdx = 1; ColumnIdx < KeyFormFactor + 1; ColumnIdx++)
        {
          SearchMapFields[ColumnIdx - 1] = mrs.getString(ColumnIdx + 1);
        }

        tmpResultList = new ArrayList<>();
        for (ColumnIdx = KeyFormFactor + 1; ColumnIdx < (ResultFormFactor + KeyFormFactor + 1); ColumnIdx++)
        {
          tmpResultList.add(mrs.getString(ColumnIdx + 1));
        }

        // Add the map
        addEntry(Group, SearchMapFields, tmpResultList);

        // Update to the log file
        if ((ConfigsLoaded % loadingLogNotificationStep) == 0)
        {
          message = "Regex Map Data Loading: <" + ConfigsLoaded +
                "> configurations loaded for <" + getSymbolicName() + "> from <" +
                cacheDataSourceName + ">";
          OpenRate.getOpenRateFrameworkLog().info(message);
        }
      }
    }
    catch (SQLException ex)
    {
      message = "Error opening Search Map Data for <" + getSymbolicName() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }
    catch (NullPointerException ex)
    {
      message = "Error opening Search Map Data for <" + getSymbolicName() + "> in config <" + ConfigsLoaded + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // Close down stuff
    try
    {
      mrs.close();
      StmtCacheDataSelectQuery.close();
      JDBCcon.close();
    }
    catch (SQLException ex)
    {
      message = "Error closing Search Map Data connection for <" +
            cacheDataSourceName + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    message = "Regex Map Data Loading completed. <" + ConfigsLoaded +
          "> configuration lines loaded for <" + getSymbolicName() + "> from <" +
          cacheDataSourceName + ">";
    OpenRate.getOpenRateFrameworkLog().info(message);
  }

 /**
  * Load the data from the defined Data Source Method
  */
  @Override
  public void loadDataFromMethod()
    throws InitializationException
  {
    int               ColumnIdx;
    int               ColumnCount;
    int               ConfigsLoaded = 0;
    String            Group;
    String[]          SearchMapFields;
    int               ResultFormFactor = 0;
    ArrayList<String> tmpMethodResult;
    ArrayList<String> tmpResultList;

    // Find the location of the  zone configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Regex Match Cache Loading from Method for <" + getSymbolicName() + ">");

    // Execute the user domain method
    Collection<ArrayList<String>> methodLoadResultSet;

    methodLoadResultSet = getMethodData(getSymbolicName(),CacheMethodName);

    if (methodLoadResultSet == null)
    {
      OpenRate.getOpenRateFrameworkLog().warning("No cache data returned by method <" + CacheMethodName +
                    "> in cache <" + getSymbolicName() + ">");
    }
    else
    {
      Iterator<ArrayList<String>> methodDataToLoadIterator = methodLoadResultSet.iterator();

      while (methodDataToLoadIterator.hasNext())
      {
        tmpMethodResult = methodDataToLoadIterator.next();

        ConfigsLoaded++;
        ColumnCount = tmpMethodResult.size();

        // Read the form factor from the first entry, and then make sure
        // that we always get this
        if (ResultFormFactor == 0)
        {
          // Check that this is a form factor we are able to use. There
          // must be at least 3 fields for us to be able to work with
          // the record:
          //  - Group identifier
          //  - Some fields to compare (min 1)
          //  - a result
          if (ColumnCount < 3)
          {
            // we're not going to be able to use this
            message = "You must define at least 3 entries in the record, you have defined  <" +
                  ColumnCount + ">";
            OpenRate.getOpenRateFrameworkLog().fatal(message);
            throw new InitializationException(message,getSymbolicName());
          }

          // Do we have a defined form factor?
          if (KeyFormFactor == 0)
          {
            // If we have not been given a key form factor, assume the result factor is 1
            ResultFormFactor = 1;
            KeyFormFactor = ColumnCount - ResultFormFactor - 1;
            message = "Using default key factor for regex cache <" +
                             cacheDataSourceName + ">. Assuming Key = <" +
                             Integer.toString(KeyFormFactor) + "> and Result = <" +
                             Integer.toString(ResultFormFactor) + ">";
            OpenRate.getOpenRateFrameworkLog().info(message);
          }
          else
          {
            // Use the given factor
            ResultFormFactor = ColumnCount - KeyFormFactor - 1;
            message = "Using defined key factor for regex cache <" +
                             cacheDataSourceName + ">. Using Key = <" +
                             Integer.toString(KeyFormFactor) + "> and Result = <" +
                             Integer.toString(ResultFormFactor) + ">";
            OpenRate.getOpenRateFrameworkLog().info(message);
          }
        }

        // Check that we are always getting the same form factor
        if (ColumnCount == (KeyFormFactor + ResultFormFactor + 1))
        {
          //Add the row, after extracting the group and the result
          Group = tmpMethodResult.get(0);

          // Get the search map fields
          SearchMapFields = new String[KeyFormFactor];
          for (ColumnIdx = 0; ColumnIdx < KeyFormFactor; ColumnIdx++)
          {
            SearchMapFields[ColumnIdx] = tmpMethodResult.get(ColumnIdx + 1);
          }

          tmpResultList = new ArrayList<>();
          for (ColumnIdx = KeyFormFactor; ColumnIdx < (ResultFormFactor + KeyFormFactor); ColumnIdx++)
          {
            tmpResultList.add(tmpMethodResult.get(ColumnIdx + 1));
          }

          // Add the map
          addEntry(Group, SearchMapFields, tmpResultList);

          // Update to the log file
          if ((ConfigsLoaded % loadingLogNotificationStep) == 0)
          {
            message = "Regex Map Data Loading: <" + ConfigsLoaded +
                  "> configurations loaded for <" + getSymbolicName() + "> from <" +
                  cacheDataSourceName + ">";
            OpenRate.getOpenRateFrameworkLog().info(message);
          }
        }
        else
        {
          // Error because the form factor changed
          message = "Form Factor should be Key = <" + Integer.toString(KeyFormFactor) +
                "> + Payload = <" + Integer.toString(ResultFormFactor) +
                ">, but received <" + tmpMethodResult.size() + ">";
          OpenRate.getOpenRateFrameworkLog().error(message);
          throw new InitializationException(message,getSymbolicName());
        }
      }
    }

    message = "Regex Map Data Loading completed. " + ConfigsLoaded +
          " configuration lines loaded for <" + getSymbolicName() + ">";
    OpenRate.getOpenRateFrameworkLog().info(message);
  }

 /**
  * Dumps the entire contents of the cache to the Log.
  */
  protected void DumpMapData()
  {
    String      		Helper;
    Iterator<String>    GroupIter;
    Iterator<SearchMap> PatternIterator;
    Iterator<String>	CDIterator;
    SearchGroup 		tmpSearchGroup;
    SearchMap   		tmpSearchMap;
    String      		PrintHelper;
    int         		counter = 0;

    OpenRate.getOpenRateFrameworkLog().info("Dumping Map Data for RegexMatchCache <" + getSymbolicName() + ">");
    OpenRate.getOpenRateFrameworkLog().info("Groups:");

    // Iterate thorough the entries in the group
    GroupIter = GroupCache.keySet().iterator();
    while (GroupIter.hasNext())
    {
      Helper = GroupIter.next();
      OpenRate.getOpenRateFrameworkLog().info("  " + Helper);
    }

    // Now dump the data
    GroupIter = GroupCache.keySet().iterator();
    while (GroupIter.hasNext())
    {
      Helper = GroupIter.next();
      OpenRate.getOpenRateFrameworkLog().info("Dumping group map data for <" + Helper + ">");
      tmpSearchGroup = GroupCache.get(Helper);
      PatternIterator = tmpSearchGroup.SearchGroup.iterator();

      while(PatternIterator.hasNext())
      {
        OpenRate.getOpenRateFrameworkLog().info("===ENTRY " + counter++ + "===");
        PrintHelper = "  (";

        tmpSearchMap = PatternIterator.next();
        for (int i = 0 ; i < tmpSearchMap.matchPattern.length ; i++)
        {
          PrintHelper = PrintHelper + "[" +
                     tmpSearchMap.matchType[i] + ":" +
                     tmpSearchMap.matchPattern[i] + ":" +
                     tmpSearchMap.matchValue[i] + "] ";
        }

        // dump the result array
        PrintHelper += ") --> (";

        CDIterator = tmpSearchMap.Results.iterator();
        while (CDIterator.hasNext())
        {
          PrintHelper = PrintHelper + CDIterator.next() + ",";
        }

        PrintHelper += ")";
        OpenRate.getOpenRateFrameworkLog().info(PrintHelper);
      }
    }
  }

 /**
  * Clear down the cache contents in the case that we are ordered to reload
  */
  @Override
  public void clearCacheObjects()
  {
    GroupCache.clear();
  }

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------

 /**
  * registerClientManager registers the client module to the ClientManager class
  * which manages all the client modules available in this OpenRate Application.
  *
  * registerClientManager registers this class as a client of the ECI listener
  * and publishes the commands that the plug in understands. The listener is
  * responsible for delivering only these commands to the plug in.
  *
  */
  @Override
  public void registerClientManager() throws InitializationException
  {
    // Set the client reference and the base services first
    super.registerClientManager();

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_GROUP_COUNT, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_OBJECT_COUNT, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_DUMP_MAP, ClientManager.PARAM_DYNAMIC);
  }

 /**
  * processControlEvent is the method that will be called when an event
  * is received for a module that has registered itself as a client of the
  * External Control Interface
  *
  * @param Command - command that is understand by the client module
  * @param Init - we are performing initial configuration if true
  * @param Parameter - parameter for the command
  * @return The result string of the operation
  */
  @Override
  public String processControlEvent(String Command, boolean Init,
                                    String Parameter)
  {
    SearchGroup tmpSearchGroup;
    Collection<String>  tmpGroups;
    Iterator<String>    GroupIter;
    String      tmpGroupName;
    int         Objects = 0;
    int         ResultCode = -1;

    // Return the number of objects in the cache
    if (Command.equalsIgnoreCase(SERVICE_GROUP_COUNT))
    {
      return Integer.toString(GroupCache.size());
    }

    if (Command.equalsIgnoreCase(SERVICE_OBJECT_COUNT))
    {
      tmpGroups = GroupCache.keySet();
      GroupIter = tmpGroups.iterator();

      while (GroupIter.hasNext())
      {
        tmpGroupName = GroupIter.next();
        tmpSearchGroup = GroupCache.get(tmpGroupName);
        Objects += tmpSearchGroup.SearchGroup.size();
      }

      return Integer.toString(Objects);
    }

    // Return the number of objects in the cache
    if (Command.equalsIgnoreCase(SERVICE_DUMP_MAP))
    {
      // onl< dump on a positive command
      if (Parameter.equalsIgnoreCase("true"))
      {
        DumpMapData();
      }

      ResultCode = 0;
    }

    if (ResultCode == 0)
    {
      OpenRate.getOpenRateFrameworkLog().debug(LogUtil.LogECICacheCommand(getSymbolicName(), Command, Parameter));

      return "OK";
    }
    else
    {
      return super.processControlEvent(Command,Init,Parameter);
    }
  }
}
