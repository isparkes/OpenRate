/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2014.
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

import OpenRate.CommonConfig;
import OpenRate.OpenRate;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.logging.LogUtil;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

/**
 * This class implements a common function of find the mapping of a given
 * resource, over time to a key. This is particularly useful for, for example
 * finding the customer that was using a router port at a given moment.
 *
 * This cache differs from the normal ValiditySegmentCache in that it allows
 * multiple periods of validity at the same moment, meaning that a lookup
 * can return multiple results.
 *
 * The format of the data, whether it is from a file or a DB is:
 *     GROUP;SHARED_RESOURCE_ID;START;END;KEY1;....;KEYN
 *     GROUP;SHARED_RESOURCE_ID;START;END;KEY1;....;KEYN
 *     ...
 *
 * The "START" and "END" fields are in Unix time format:
 *
 * e.g.RADIUS;PORT_10_3;1159981449;1159983125;CBA1023452
 * e.g.RADIUS;PORT_12_0;1159783248;1159823217;CBA1334467
 *     ...
 *
 * The data can be loaded either from a file, or from a DB. The properties that
 * must be configured for these are slightly different in each case.
 *
 * Loading from a file:
 * --------------------
 *   Define "DataSoureceType" as "File"
 *   Define "DataFile" to point to the (relative or absolute) location of the
 *     file to load
 *
 * CacheableClass.0.ClassName=OpenRate.cache.MultipleValidityCache
 * CacheableClass.0.ModuleName=PortCache
 * PortCache.DataSourceType=File
 * PortCache.DataFile=ConfigData/Router/PortMapFile.dat
 *
 * Loading from a DB:
 * ------------------
 *   Define "DataSourecType" as "DB"
 *   Define "DataSource" to point to the data source name to load from
 *   Define "SelectStatement" to return the data you wish to retrieve
 *
 * CacheableClass.0.ClassName=OpenRate.cache.MultipleValidityCache
 * CacheableClass.0.ModuleName=PortCache
 * PortCache.DataSourceType=DB
 * PortCache.DataSource=LookupDataSource
 * PortCache.SelectStatement=select "DEFAULT",PORT_DESC,VALID_FROM,VALID_TO,CBA_ID from PORT_TAB
 */

public class MultipleValidityCache
     extends AbstractSyncLoaderCache
{
  // This stores the index to all the groups.
  private HashMap<String, HashMap<String, ValidityNode>> GroupCache;

 /**
  * A ValidityNode is a segment of validity of a resource. These are chained
  * together in a sorted linked list. The sorting is done at insertion time
  * into the list, meaning that lookups at run time can be optimised.
  */
  private class ValidityNode
  {
    long              timeFrom;
    long              timeTo;
    ArrayList<String> results = null;
    ValidityNode      child = null;
  }

  // List of Services that this Client supports
  private final static String SERVICE_GROUP_COUNT = "GroupCount";
  private final static String SERVICE_OBJECT_COUNT = "ObjectCount";

    /**
     * The default return when there is no match
     */
    public static final String NO_VALIDITY_MATCH = "NOMATCH";

 /**
  * Creates a new instance of the group Cache. The group Cache contains all
  * of the Groups that are later cached. The lookup is therefore performed within
  * the group, retrieving the validity segment for that group, resource_id and
  * time.
  */
  public MultipleValidityCache()
  {
    super();

    GroupCache = new HashMap<>(50);
  }

 /**
  * Add a value into the Validity Segment Cache, defining the result
  * value that should be returned in the case of a match.
  * The entries are ordered during the loading in a linked list sorted by
  * validity date. This makes the search at run time easier.
  *
  * @param group The data group to add the entry to
  * @param resourceID The resourceID of the entry to add
  * @param startTime The start time of the validity
  * @param endTime The end time of the validity
  * @param results The list of result values
  */
  public void addEntry(String group, String resourceID, long startTime,
                       long endTime, ArrayList<String> results)
  {
    HashMap<String, ValidityNode>      tmpResourceCache;
    ValidityNode tmpValidityNode;
    ValidityNode newNode;

    // See if we already have the group cache for this Group
    if (!GroupCache.containsKey(group))
    {
      // Create the new resource cache
      tmpResourceCache = new HashMap<>(100);

      // Add it to the group cache
      GroupCache.put(group, tmpResourceCache);
    }
    else
    {
      // Otherwise just get the existing object
      tmpResourceCache = GroupCache.get(group);
    }

    // Now add the validity segment into the list, checking only the start
    // time (thuis avoiding the overlap detection
    if (!tmpResourceCache.containsKey(resourceID))
    {
      // Create the new list
      tmpValidityNode = new ValidityNode();
      tmpValidityNode.timeFrom = startTime;
      tmpValidityNode.timeTo = endTime;
      tmpValidityNode.results = results;
      tmpValidityNode.child = null;

      // Add in the new node
      tmpResourceCache.put(resourceID, tmpValidityNode);
    }
    else
    {
      // Recover the validity map that there is
      tmpValidityNode = tmpResourceCache.get(resourceID);

      // now run down the validity entries until we get to the end
      while (tmpValidityNode.child != null)
      {
        tmpValidityNode = tmpValidityNode.child;
      }

      // add the node - we don't care about order
      newNode = new ValidityNode();

      // move the information over
      newNode.timeFrom = startTime;
      newNode.timeTo = endTime;
      newNode.results = results;
      newNode.child = null;

      // add the new information to the tail of the list
      tmpValidityNode.child = newNode;
    }
  }

 /**
  * Returns the first entry matching the resourceID in the given
  * group at the given time
  *
  * @param group The resource group to search in
  * @param resourceID The resource identifier to search for
  * @param time The time to search for
  * @return The retrieved value, or "NOMATCH" if none found
  */
  public String getFirstValidityMatch(String group, String resourceID, long time)
  {
    HashMap<String,ValidityNode> tmpResourceCache;
    ValidityNode                 tmpValidityNode;

    // Get the service if we know it
    tmpResourceCache = GroupCache.get(group);

    if (tmpResourceCache != null)
    {
      tmpValidityNode = tmpResourceCache.get(resourceID);

      // Now that we have the Validity Map, get the entry
      while (tmpValidityNode != null)
      {
        if ((tmpValidityNode.timeFrom <= time) &
            (tmpValidityNode.timeTo > time))
        {
          return tmpValidityNode.results.get(0);
        }

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }
    }

    return NO_VALIDITY_MATCH;
  }

 /**
  * Returns the result vector matching the resourceID in the given
  * group at the given time
  *
  * @param group The resource group to search in
  * @param resourceID The resource identifier to search for
  * @param time The time to search for
  * @return The retrieved value vector, or null if none found
  */
  public ArrayList<String> getFirstValidityMatchWithChildData(String group, String resourceID, long time)
  {
    HashMap<String,ValidityNode> tmpResourceCache;
    ValidityNode   			     tmpValidityNode;
    ArrayList<String> Value = null;

    // Get the service if we know it
    tmpResourceCache = GroupCache.get(group);

    if (tmpResourceCache != null)
    {
      tmpValidityNode = tmpResourceCache.get(resourceID);

      // Now that we have the Validity Map, get the entry
      while (tmpValidityNode != null)
      {
        if ((tmpValidityNode.timeFrom <= time) &
            (tmpValidityNode.timeTo > time))
        {
          return tmpValidityNode.results;
        }

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }
    }

    return Value;
  }

 /**
  * Returns the vector of all matches to the resourceID in the given
  * group at the given time
  *
  * @param group The resource group to search in
  * @param resourceID The resource identifier to search for
  * @param time The time to search for
  * @return The retrieved value vector, or null if none found
  */
  public ArrayList<String> getAllValidityMatches(String group, String resourceID, long time)
  {
    HashMap<String,ValidityNode> tmpResourceCache;
    ValidityNode      			 tmpValidityNode;
    ArrayList<String> returnValue = new ArrayList<>();

    // Get the service if we know it
    tmpResourceCache = GroupCache.get(group);

    if (tmpResourceCache != null)
    {
      tmpValidityNode = tmpResourceCache.get(resourceID);

      // Now that we have the Validity Map, get the entry
      while (tmpValidityNode != null)
      {
        if ((tmpValidityNode.timeFrom <= time) &
            (tmpValidityNode.timeTo > time))
        {
          // Add the value to the results list
          returnValue.add(tmpValidityNode.results.get(0));
        }

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }
    }

    return returnValue;
  }

 /**
  * Returns the vector of all matches to the resourceID in the given
  * group at the given time
  *
  * @param group The resource group to search in
  * @param resourceID The resource identifier to search for
  * @param time The time to search for
  * @return The retrieved value vector, or null if none found
  */
  public ArrayList<ArrayList<String>> getAllValidityMatchesWithChildData(String group, String resourceID, long time)
  {
    HashMap<String,ValidityNode> tmpResourceCache;
    ValidityNode                 tmpValidityNode;
    ArrayList<ArrayList<String>> returnValue = new ArrayList<>();

    // Get the service if we know it
    tmpResourceCache = GroupCache.get(group);

    if (tmpResourceCache != null)
    {
      tmpValidityNode = tmpResourceCache.get(resourceID);

      // Now that we have the Validity Map, get the entry
      while (tmpValidityNode != null)
      {
        if ((tmpValidityNode.timeFrom <= time) &
            (tmpValidityNode.timeTo > time))
        {
          // Add the value to the results list
          returnValue.add(tmpValidityNode.results);
        }

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }
    }

    return returnValue;
  }

// -----------------------------------------------------------------------------
// ------------------ Start of inherited Plug In functions ---------------------
// -----------------------------------------------------------------------------

 /**
  * Load the data from the defined file
  */
  @Override
  public synchronized void loadDataFromFile() throws InitializationException
  {
    // Variable declarations
    int               validityPeriodsLoaded = 0;
    BufferedReader    inFile;
    String            tmpFileRecord;
    String[]          zoneFields;
    String            group;
    String            resourceID;
    long              timeFrom;
    long              timeTo;
    ArrayList<String> result;
    int               idx;
    String            tmpStartDate = null;
    String            tmpEndDate = null;

    // Find the location of the  zone configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Multiple Validity Cache Loading from File");

    // Try to open the file
    try
    {
      inFile = new BufferedReader(new FileReader(cacheDataFile));
    }
    catch (FileNotFoundException ex)
    {
      message = "Application is not able to read file : <" +
            cacheDataSourceName + ">";
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
          // Comment line or whitespace line, ignore
        }
        else
        {
          validityPeriodsLoaded++;
          zoneFields = tmpFileRecord.split(";");
          group = zoneFields[0];
          resourceID = zoneFields[1];
          tmpStartDate = zoneFields[2];
          tmpEndDate = zoneFields[3];
          timeFrom = (int) fieldInterpreter.convertInputDateToUTC(tmpStartDate);
          timeTo = (int) fieldInterpreter.convertInputDateToUTC(tmpEndDate);

          // Interpret 0 values
          if (timeFrom == 0) timeFrom = CommonConfig.LOW_DATE;
          if (timeTo == 0) timeFrom = CommonConfig.HIGH_DATE;

          // now make an ArrayList of the results
          result = new ArrayList<>();
          for (idx = 4 ; idx < zoneFields.length ; idx++)
          {
            result.add(zoneFields[idx]);
          }

          // Interpret 0 values
          if (timeFrom == 0)
          {
            timeFrom = CommonConfig.LOW_DATE;
          }

          if (timeTo == 0)
          {
            timeTo = CommonConfig.HIGH_DATE;
          }

          addEntry(group, resourceID, timeFrom, timeTo, result);

          // Update to the log file
          if ((validityPeriodsLoaded % loadingLogNotificationStep) == 0)
          {
            message = "Multiple Validity Data Loading: <" + validityPeriodsLoaded +
                  "> configurations loaded for <" + getSymbolicName() + "> from <" +
                  cacheDataFile + ">";
            OpenRate.getOpenRateFrameworkLog().info(message);
          }
        }
      }
    }
    catch (IOException ex)
    {
      OpenRate.getOpenRateFrameworkLog().fatal(
            "Error reading input file <" + cacheDataFile +
            "> in record <" + validityPeriodsLoaded + ">. IO Error.");
    }
    catch (ArrayIndexOutOfBoundsException ex)
    {
      OpenRate.getOpenRateFrameworkLog().fatal(
            "Error reading input file <" + cacheDataFile +
            "> in record <" + validityPeriodsLoaded + ">. Malformed Record.");
    }
    catch (ParseException pe)
    {
      message =
            "Error converting date from <" + getSymbolicName() + "> in record <" +
            validityPeriodsLoaded + ">. Unexpected date value <" + tmpStartDate +
            ">, <" + tmpEndDate + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    finally
    {
      try
      {
        inFile.close();
      }
      catch (IOException ex)
      {
        OpenRate.getOpenRateFrameworkLog().error(
              "Error closing input file <" + cacheDataFile +
              ">", ex);
      }
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Multiple Validity Map Data Loading completed. " +
          validityPeriodsLoaded + " configuration lines loaded from <" +
          cacheDataFile + ">");
  }

 /**
  * Load the data from the defined Data Source. This method is synchronised to avoid re-entrancy
  * problems with auto-loading.
  */
  @Override
  public synchronized void loadDataFromDB()
                      throws InitializationException
  {
    int               validityPeriodsLoaded = 0;
    String            group;
    String            resourceID;
    long              timeFrom;
    long              timeTo;
    ArrayList<String> result;
    int               idx;
    String            tmpStartDate = null;
    String            tmpEndDate = null;

    // Find the location of the  zone configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Multiple Validity Cache Loading from DB in <" + getSymbolicName() + ">");

    try
    {
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
        message = "Error performing SQL for retrieving Multiple Validity Match data in <" + getSymbolicName() + ">. message = <" + ex.getMessage() + ">";
        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message,getSymbolicName());
      }

      // loop through the results for the customer login cache
      try
      {
        mrs.beforeFirst();

        while (mrs.next())
        {
          validityPeriodsLoaded++;
          group = mrs.getString(1);
          resourceID = mrs.getString(2);
          tmpStartDate = mrs.getString(3);
          tmpEndDate = mrs.getString(4);
          timeFrom = (int) fieldInterpreter.convertInputDateToUTC(tmpStartDate);
          timeTo = (int) fieldInterpreter.convertInputDateToUTC(tmpEndDate);

          // Interpret 0 values
          if (timeFrom == 0) timeFrom = CommonConfig.LOW_DATE;
          if (timeTo == 0) timeTo = CommonConfig.HIGH_DATE;

          // now make an ArrayList of the results
          result = new ArrayList<>();
          for (idx = 5 ; idx <= mrs.getMetaData().getColumnCount() ; idx++)
          {
            result.add(mrs.getString(idx));
          }

          // Add the map
          addEntry(group, resourceID, timeFrom, timeTo, result);

          // Update to the log file
          if ((validityPeriodsLoaded % loadingLogNotificationStep) == 0)
          {
            message = "Multiple Validity Data Loading: <" + validityPeriodsLoaded +
                  "> configurations loaded for <" + getSymbolicName() + "> from <" +
                  cacheDataSourceName + ">";
            OpenRate.getOpenRateFrameworkLog().info(message);
          }
        }
      }
      catch (SQLException ex)
      {
        message = "Error opening Multiple Validity Match Data for <" + getSymbolicName() + ">. message = <" + ex.getMessage() +">";
        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message,getSymbolicName());
      }
      catch (ParseException pe)
      {
        message =
              "Error converting date from <" + getSymbolicName() + "> in record <" +
              validityPeriodsLoaded + ">. Unexpected date value <" + tmpStartDate +
              ">, <" + tmpEndDate + ">. message = <" + pe.getMessage() + ">";
        OpenRate.getOpenRateFrameworkLog().fatal(message,pe);
        throw new InitializationException(message,getSymbolicName());
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
        message = "Error closing Multiple Validity Match Data for <" + getSymbolicName() + ">. message = <" + ex.getMessage() +">";
        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message,getSymbolicName());
      }
    }
    finally
    {
      DBUtil.close(mrs);
      DBUtil.close(StmtCacheDataSelectQuery);
      DBUtil.close(JDBCcon);
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Multiple Validity Map Data Loading completed. " +
          validityPeriodsLoaded + " configuration lines loaded from <" +
          cacheDataSourceName + ">");
  }

 /**
  * Load the data from the defined Data Source Method
  */
  @Override
  public void loadDataFromMethod()
                      throws InitializationException
  {
    // Variable declarations
    int               validityPeriodsLoaded = 0;
    int               formFactor;
    String            group;
    String            resourceID;
    long              timeFrom;
    long              timeTo;
    ArrayList<String> result;
    int               idx;
    ArrayList<String> tmpMethodResult;

    // Find the location of the  zone configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Multiple Validity Cache Loading from Method for <" + getSymbolicName() + ">");

    // Execute the user domain method
    Collection<ArrayList<String>> methodLoadResultSet;

    methodLoadResultSet = getMethodData(getSymbolicName(),CacheMethodName);

    Iterator<ArrayList<String>> methodDataToLoadIterator = methodLoadResultSet.iterator();

    // loop through the results for the customer login cache
    while (methodDataToLoadIterator.hasNext())
    {
      validityPeriodsLoaded++;
      tmpMethodResult = methodDataToLoadIterator.next();
      formFactor = tmpMethodResult.size();

      if (formFactor < 5)
      {
        // There are not enough fields
        message = "Error reading input data from <" + cacheDataSourceName +
        "> in record <" + validityPeriodsLoaded + ">. Not enough fields.";

        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message,getSymbolicName());
      }

      group = tmpMethodResult.get(0);
      resourceID = tmpMethodResult.get(1);
      timeFrom = Long.valueOf(tmpMethodResult.get(2));
      timeTo = Long.valueOf(tmpMethodResult.get(3));

      // Interpret 0 values
      if (timeFrom == 0) timeFrom = CommonConfig.LOW_DATE;
      if (timeTo == 0) timeTo = CommonConfig.HIGH_DATE;

      // now make an ArrayList of the results
      result = new ArrayList<>();
      for (idx = 4 ; idx < tmpMethodResult.size() ; idx++)
      {
        result.add(tmpMethodResult.get(idx));
      }

      // deal with high dates
      if (timeTo == 0)
      {
        timeTo = CommonConfig.HIGH_DATE;
      }

      addEntry(group, resourceID, timeFrom, timeTo, result);

      // Update to the log file
      if ((validityPeriodsLoaded % loadingLogNotificationStep) == 0)
      {
        message = "Multiple Validity Data Loading: <" + validityPeriodsLoaded +
              "> configurations loaded for <" + getSymbolicName() + "> from <" +
              cacheDataSourceName + ">";
        OpenRate.getOpenRateFrameworkLog().info(message);
      }
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Multiple Validity Map Data Loading completed. " +
          validityPeriodsLoaded + " configuration lines loaded from <" +
          cacheDataSourceName + ">");
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
  }

 /**
  * processControlEvent is the method that will be called when an event
  * is received for a module that has registered itself as a client of the
  * External Control Interface
  *
  * @param Command - command that is understand by the client module
  * @param init - we are performing initial configuration if true
  * @param parameter - parameter for the command
  * @return The result string of the operation
  */
  @Override
  public String processControlEvent(String command, boolean init,
                                    String parameter)
  {
    HashMap<String,ValidityNode> tmpResource;
    Collection<String>           tmpGroups;
    Iterator<String>             groupIter;
    String                       tmpGroupName;
    int                          objects = 0;
    int                          resultCode = -1;

    // Return the number of objects in the cache
    if (command.equalsIgnoreCase(SERVICE_GROUP_COUNT))
    {
      return Integer.toString(GroupCache.size());
    }

    if (command.equalsIgnoreCase(SERVICE_OBJECT_COUNT))
    {
      tmpGroups = GroupCache.keySet();
      groupIter = tmpGroups.iterator();

      while (groupIter.hasNext())
      {
        tmpGroupName = groupIter.next();
        tmpResource = GroupCache.get(tmpGroupName);
        objects += tmpResource.size();
      }

      return Integer.toString(objects);
    }

    if (resultCode == 0)
    {
      OpenRate.getOpenRateFrameworkLog().debug(LogUtil.LogECICacheCommand(getSymbolicName(), command, parameter));

      return "OK";
    }
    else
    {
      return super.processControlEvent(command,init,parameter);
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
}

