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
 * CacheableClass.0.ClassName=OpenRate.cache.ValiditySegmentCache
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
 * CacheableClass.0.ClassName=OpenRate.cache.ValiditySegmentCache
 * CacheableClass.0.ModuleName=PortCache
 * PortCache.DataSourceType=DB
 * PortCache.DataSource=LookupDataSource
 * PortCache.SelectStatement=select "DEFAULT",PORT_DESC,VALID_FROM,VALID_TO,CBA_ID from PORT_TAB
 */

public class ValiditySegmentCache
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
    long           TimeFrom;
    long           TimeTo;
    ArrayList<String> Results = null;
    ValidityNode   child = null;
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
  * the group, retrieveing the validity segment for that group, resource_id and
  * time.
  */
  public ValiditySegmentCache()
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
  * @param Group The data group to add the entry to
  * @param ResourceID The resourceID of the entry to add
  * @param StartTime The start time of the validity
  * @param EndTime The end time of the validity
  * @param Results The list of result values
  */
  public void addEntry(String Group, String ResourceID, long StartTime,
                       long EndTime, ArrayList<String> Results)
  {
    long         tmpTimeFrom;
    long         lastValidTo;
    HashMap<String, ValidityNode>      tmpResourceCache;
    ValidityNode tmpValidityNode;
    ValidityNode tmpValidityNextNode;
    ValidityNode newNode;

    // See if we already have the group cache for this Group
    if (!GroupCache.containsKey(Group))
    {
      // Create the new resource cache
      tmpResourceCache = new HashMap<>(100);

      // Add it to the group cache
      GroupCache.put(Group, tmpResourceCache);
    }
    else
    {
      // Otherwise just get the existing object
      tmpResourceCache = GroupCache.get(Group);
    }

    // Now add the validity segment into the list
    if (!tmpResourceCache.containsKey(ResourceID))
    {
      // Create the new list
      tmpValidityNode = new ValidityNode();
      tmpValidityNode.TimeFrom = StartTime;
      tmpValidityNode.TimeTo = EndTime;
      tmpValidityNode.Results = Results;
      tmpValidityNode.child = null;

      // Add in the new node
      tmpResourceCache.put(ResourceID, tmpValidityNode);
    }
    else
    {
      // Recover the validity map that there is
      tmpValidityNode = tmpResourceCache.get(ResourceID);

      // preset our valid to date
      lastValidTo = CommonConfig.LOW_DATE;

      // now run down the validity periods until we find the right position
      while (tmpValidityNode != null)
      {
        tmpValidityNextNode = tmpValidityNode.child;

        if (tmpValidityNextNode != null)
        {
          tmpTimeFrom = tmpValidityNextNode.TimeFrom;
        }
        else
        {
          tmpTimeFrom = CommonConfig.HIGH_DATE;
        }

        if ((StartTime > tmpValidityNode.TimeTo) &
            (tmpValidityNextNode == null))
        {
          // insert at the tail of the list if we are able
          newNode = new ValidityNode();
          tmpValidityNode.child = newNode;
          newNode.TimeFrom = StartTime;
          newNode.TimeTo = EndTime;
          newNode.Results = Results;

          // done
          return;
        }
        else if ((StartTime > lastValidTo) &
            (EndTime <= tmpValidityNode.TimeFrom))
        {
          // insert at the head of the list
          newNode = new ValidityNode();

          // move the information over
          newNode.TimeFrom = tmpValidityNode.TimeFrom;
          newNode.TimeTo = tmpValidityNode.TimeTo;
          newNode.Results = tmpValidityNode.Results;
          newNode.child = tmpValidityNode.child;

          // add the new information
          tmpValidityNode.TimeFrom = StartTime;
          tmpValidityNode.TimeTo = EndTime;
          tmpValidityNode.Results = Results;
          tmpValidityNode.child = newNode;

          // done
          return;
        }
        else if ((StartTime > tmpValidityNode.TimeTo) & (EndTime <= tmpTimeFrom))
        {
          // insert in the middle of the list
          newNode = new ValidityNode();
          newNode.child = tmpValidityNode.child;
          tmpValidityNode.child = newNode;
          newNode.TimeFrom = StartTime;
          newNode.TimeTo = EndTime;
          newNode.Results = Results;

          return;
        }

        // Move down the map
        lastValidTo = tmpValidityNode.TimeTo;
        tmpValidityNode = tmpValidityNode.child;
      }

      // If we get here, we could not insert the period
      OpenRate.getOpenRateFrameworkLog().error("Cache <" + getSymbolicName() +
            "> could not insert <" + Group + ":" + ResourceID + ":" +
            StartTime + ":" + EndTime + "> without overlap.");
    }
  }

 /**
  * Returns the entry matching the resourceID in the given
  * group at the given time
  *
  * @param Group The resource group to search in
  * @param ResourceID The resource identifier to search for
  * @param Time The time to search for
  * @return The retrieved value, or "NOMATCH" if none found
  */
  public String getValiditySegmentMatch(String Group, String ResourceID, long Time)
  {
    HashMap<String, ValidityNode>        tmpResourceCache;
    ValidityNode   tmpValidityNode;

    // Get the service if we know it
    tmpResourceCache = GroupCache.get(Group);

    if (tmpResourceCache != null)
    {
      tmpValidityNode = tmpResourceCache.get(ResourceID);

      // Now that we have the Validity Map, get the entry
      while (tmpValidityNode != null)
      {
        if ((tmpValidityNode.TimeFrom <= Time) &
            (tmpValidityNode.TimeTo > Time))
        {
          return tmpValidityNode.Results.get(0);
        }

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }
    }

    return NO_VALIDITY_MATCH;
  }

 /**
  * Returns the vector matching the resourceID in the given
  * group at the given time
  *
  * @param Group The resource group to search in
  * @param ResourceID The resource identifier to search for
  * @param Time The time to search for
  * @return The retrieved value vector, or null if none found
  */
  public ArrayList<String> getValiditySegmentMatchWithChildData(String Group, String ResourceID, long Time)
  {
    HashMap<String, ValidityNode>        tmpResourceCache;
    ValidityNode   tmpValidityNode;
    ArrayList<String> Value = null;

    // Get the service if we know it
    tmpResourceCache = GroupCache.get(Group);

    if (tmpResourceCache != null)
    {
      tmpValidityNode = tmpResourceCache.get(ResourceID);

      // Now that we have the Validity Map, get the entry
      while (tmpValidityNode != null)
      {
        if ((tmpValidityNode.TimeFrom <= Time) &
            (tmpValidityNode.TimeTo > Time))
        {
          return tmpValidityNode.Results;
        }

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }
    }

    return Value;
  }

// -----------------------------------------------------------------------------
// ------------------ Start of inherited Plug In functions ---------------------
// -----------------------------------------------------------------------------

 /**
  * Load the data from the defined file
  */
  @Override
  public void loadDataFromFile()
    throws InitializationException
  {
    // Variable declarations
    int               ValidityPeriodsLoaded = 0;
    BufferedReader    inFile;
    String            tmpFileRecord;
    String[]          ZoneFields;
    String            Group;
    String            ResourceID;
    long              TimeFrom;
    long              TimeTo;
    ArrayList<String> Result;
    int               Index;
    String            tmpStartDate = null;
    String            tmpEndDate = null;

    // Find the location of the  zone configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Validity Segment Cache Loading from File");

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
          ValidityPeriodsLoaded++;
          ZoneFields = tmpFileRecord.split(";");
          Group = ZoneFields[0];
          ResourceID = ZoneFields[1];
          tmpStartDate = ZoneFields[2];
          tmpEndDate = ZoneFields[3];
          TimeFrom = (int) fieldInterpreter.convertInputDateToUTC(tmpStartDate);
          TimeTo = (int) fieldInterpreter.convertInputDateToUTC(tmpEndDate);

          // Interpret 0 values
          if (TimeFrom == 0) TimeFrom = CommonConfig.LOW_DATE;
          if (TimeTo == 0) TimeFrom = CommonConfig.HIGH_DATE;

          // now make an ArrayList of the results
          Result = new ArrayList<>();
          for (Index = 4 ; Index < ZoneFields.length ; Index++)
          {
            Result.add(ZoneFields[Index]);
          }

          // Interpret 0 values
          if (TimeFrom == 0) TimeFrom = CommonConfig.LOW_DATE;
          if (TimeTo == 0) TimeTo = CommonConfig.HIGH_DATE;

          addEntry(Group, ResourceID, TimeFrom, TimeTo, Result);

          // Update to the log file
          if ((ValidityPeriodsLoaded % loadingLogNotificationStep) == 0)
          {
            message = "Validity Segment Map Data Loading: <" + ValidityPeriodsLoaded +
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
            "> in record <" + ValidityPeriodsLoaded + ">. IO Error.");
    }
    catch (ArrayIndexOutOfBoundsException ex)
    {
      OpenRate.getOpenRateFrameworkLog().fatal(
            "Error reading input file <" + cacheDataFile +
            "> in record <" + ValidityPeriodsLoaded + ">. Malformed Record.");
    }
    catch (ParseException pe)
    {
      message =
            "Error converting date from <" + getSymbolicName() + "> in record <" +
            ValidityPeriodsLoaded + ">. Unexpected date value <" + tmpStartDate +
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
          "Validity Segment Map Data Loading completed. " +
          ValidityPeriodsLoaded + " configuration lines loaded from <" +
          cacheDataFile + ">");
  }

 /**
  * Load the data from the defined Data Source
  */
  @Override
  public void loadDataFromDB()
                      throws InitializationException
  {
    int               ValidityPeriodsLoaded = 0;
    String            Group;
    String            ResourceID;
    long              TimeFrom;
    long              TimeTo;
    ArrayList<String> Result;
    int               Index;
    String            tmpStartDate = null;
    String            tmpEndDate = null;

    // Find the location of the  zone configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Validity Segment Cache Loading from DB in <" + getSymbolicName() + ">");

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
      message = "Error performing SQL for retrieving Validity Segment Match data in <" + getSymbolicName() + ">. message = <" + ex.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // loop through the results for the customer login cache
    try
    {
      mrs.beforeFirst();

      while (mrs.next())
      {
        ValidityPeriodsLoaded++;
        Group = mrs.getString(1);
        ResourceID = mrs.getString(2);
        tmpStartDate = mrs.getString(3);
        tmpEndDate = mrs.getString(4);
        TimeFrom = (int) fieldInterpreter.convertInputDateToUTC(tmpStartDate);
        TimeTo = (int) fieldInterpreter.convertInputDateToUTC(tmpEndDate);

        // Interpret 0 values
        if (TimeFrom == 0) TimeFrom = CommonConfig.LOW_DATE;
        if (TimeTo == 0) TimeTo = CommonConfig.HIGH_DATE;

        // now make an ArrayList of the results
        Result = new ArrayList<>();
        for (Index = 5 ; Index <= mrs.getMetaData().getColumnCount() ; Index++)
        {
          Result.add(mrs.getString(Index));
        }

        // Add the map
        addEntry(Group, ResourceID, TimeFrom, TimeTo, Result);

        // Update to the log file
        if ((ValidityPeriodsLoaded % loadingLogNotificationStep) == 0)
        {
          message = "Validity Segment Map Data Loading: <" + ValidityPeriodsLoaded +
                "> configurations loaded for <" + getSymbolicName() + "> from <" +
                cacheDataSourceName + ">";
          OpenRate.getOpenRateFrameworkLog().info(message);
        }
      }
    }
    catch (SQLException ex)
    {
      message = "Error opening Validity Segment Match Data for <" + getSymbolicName() + ">. message = <" + ex.getMessage() +">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }
    catch (ParseException pe)
    {
      message =
            "Error converting date from <" + getSymbolicName() + "> in record <" +
            ValidityPeriodsLoaded + ">. Unexpected date value <" + tmpStartDate +
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
      message = "Error closing Validity Segment Match Data for <" + getSymbolicName() + ">. message = <" + ex.getMessage() +">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Validity Segment Map Data Loading completed. " +
          ValidityPeriodsLoaded + " configuration lines loaded from <" +
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
    int            ValidityPeriodsLoaded = 0;
    int            formFactor;
    String         Group;
    String         ResourceID;
    long           TimeFrom;
    long           TimeTo;
    ArrayList<String> Result;
    int            Index;
    ArrayList<String> tmpMethodResult;

    // Find the location of the  zone configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Validity Segment Cache Loading from Method for <" + getSymbolicName() + ">");

    // Execute the user domain method
    Collection<ArrayList<String>> methodLoadResultSet;

    methodLoadResultSet = getMethodData(getSymbolicName(),CacheMethodName);

    Iterator<ArrayList<String>> methodDataToLoadIterator = methodLoadResultSet.iterator();

    // loop through the results for the customer login cache
    while (methodDataToLoadIterator.hasNext())
    {
      ValidityPeriodsLoaded++;
      tmpMethodResult = methodDataToLoadIterator.next();
      formFactor = tmpMethodResult.size();

      if (formFactor < 5)
      {
        // There are not enough fields
        message = "Error reading input data from <" + cacheDataSourceName +
        "> in record <" + ValidityPeriodsLoaded + ">. Not enough fields.";

        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message,getSymbolicName());
      }

      Group = tmpMethodResult.get(0);
      ResourceID = tmpMethodResult.get(1);
      TimeFrom = Integer.valueOf(tmpMethodResult.get(2));
      TimeTo = Integer.valueOf(tmpMethodResult.get(3));

      // Interpret 0 values
      if (TimeFrom == 0) TimeFrom = CommonConfig.LOW_DATE;
      if (TimeTo == 0) TimeTo = CommonConfig.HIGH_DATE;

      // now make an ArrayList of the results
      Result = new ArrayList<>();
      for (Index = 4 ; Index < tmpMethodResult.size() ; Index++)
      {
        Result.add(tmpMethodResult.get(Index));
      }

      // deal with high dates
      if (TimeTo == 0)
      {
        TimeTo = CommonConfig.HIGH_DATE;
      }

      addEntry(Group, ResourceID, TimeFrom, TimeTo, Result);

      // Update to the log file
      if ((ValidityPeriodsLoaded % loadingLogNotificationStep) == 0)
      {
        message = "Validity Segment Map Data Loading: <" + ValidityPeriodsLoaded +
              "> configurations loaded for <" + getSymbolicName() + "> from <" +
              cacheDataSourceName + ">";
        OpenRate.getOpenRateFrameworkLog().info(message);
      }
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Validity Segment Map Data Loading completed. " +
          ValidityPeriodsLoaded + " configuration lines loaded from <" +
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
  * @param Init - we are performing initial configuration if true
  * @param Parameter - parameter for the command
  * @return The result string of the operation
  */
  @Override
  public String processControlEvent(String Command, boolean Init,
                                    String Parameter)
  {
    HashMap<String, ValidityNode> tmpResource;
    Collection<String> tmpGroups;
    Iterator<String> GroupIter;
    String tmpGroupName;
    int Objects = 0;
    int ResultCode = -1;

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
        tmpResource = GroupCache.get(tmpGroupName);
        Objects += tmpResource.size();
      }

      return Integer.toString(Objects);
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

 /**
  * Clear down the cache contents in the case that we are ordered to reload
  */
  @Override
  public void clearCacheObjects()
  {
    GroupCache.clear();
  }
}

