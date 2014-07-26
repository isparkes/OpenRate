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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

/**
 * This class implements an in-memory lookup, looking a number up in a range,
 * and returning the appropriate result. This is useful for looking up if a
 * number belongs to a range of numbers.
 *
 * This example will load the map data from a file or a DB, and expects at
 * least 6 fields:
 *  - Group            - grouping key for subdividing information
 *  - Range from       - start of the number range
 *  - Range to         - end of the number range
 *  - Time from        - start of the validity period
 *  - Time to          - end of the validity period
 *  - Result field 1   - result (mandatory)
 *  [- Result field 2] - more results (optional)
 *  [- Result field n] - more results (optional)
 *
 * "Range From" must always be less than or equal to "Range To". Overlapping
 * ranges in a group are not allowed.
 *
 * Loading from a file:
 * --------------------
 *   Define "DataSourecType" as "File"
 *   Define "DataFile" to point to the (relative or absolute) location of the
 *     file to load
 *
 * Loading from a DB:
 * ------------------
 *   Define "DataSourecType" as "DB"
 *   Define "DataSource" to point to the data source name to load from
 *   Define "SelectStatement" to return the data you wish to retrieve
 *
 */
public class NumberRangeCache
     extends AbstractSyncLoaderCache
{
  // This is the management structure which allows us to order the ranges
  private class RangeItem
  {
    long RangeFrom;
    long RangeTo;
    long ValidityFrom;
    long ValidityTo;
    RangeItem nextRange;
    ArrayList<String> Results;
  }

  /**
   * This stores the index to all the groups. Groups are used to subdivide the
   * entries in the cache in order that we do not need to search through all
   * the global possibilities to find ours. We only have to search through the
   * group of similar entries.
   */
  private HashMap<String, RangeItem> GroupCache;

  // List of Services that this Client supports
  private final static String SERVICE_OBJECT_COUNT = "ObjectCount";
  private final static String SERVICE_GROUP_COUNT = "GroupCount";

  /**
   * The default return when there is no match
   */
  public static final String NO_RANGE_MATCH = "NOMATCH";

 /**
  * Creates a new instance of the Indexed Match Cache. The Cache contains all
  * of the Objects that are later cached. The lookup is performed using the
  * indexes that created at loading time.
  */
  public NumberRangeCache()
  {
    super();

    GroupCache = new HashMap<>(500);
  }

// -----------------------------------------------------------------------------
// ------------------ Start of inherited Plug In functions ---------------------
// -----------------------------------------------------------------------------

 /**
  * Add an object into the Object Cache, creating a chain of values to search,
  * ordered by "RangeFrom".
  * @param Group The group to add the entry to
  * @param ValidityFrom The start of the validity of the range
  * @param ValidityTo The end of the validity of the range
  * @param RangeFrom The start of the range
  * @param RangeTo The end of the range
  * @param Results The results associated with this range
  * @throws InitializationException
  */
  public void addEntry(String Group, long RangeFrom, long RangeTo, long ValidityFrom, long ValidityTo, ArrayList<String> Results)
          throws InitializationException
  {
    RangeItem tmpRangeItem;
    RangeItem newRangeItem;
    RangeItem tmpRangeNextNode;

    // these hold the modified values
    long tmpRF = RangeFrom;
    long tmpRT = RangeTo;
    long tmpVF = ValidityFrom;
    long tmpVT = ValidityTo;

    // check that the range is OK
    if (RangeFrom > RangeTo)
    {
      message = "Range From <" + RangeFrom +
              "> cannot be larger than Range To <" + RangeTo + "> in group <" +
              Group + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    if ((ValidityFrom > ValidityTo) && (ValidityTo > 0))
    {
      message = "Validity From <" + ValidityFrom +
              "> cannot be larger than Validity To <" + ValidityTo +
              "> in group <" + Group + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // make sure that we deal with the "don't care" cases
    if (RangeFrom == 0)
    {
      tmpRF = Long.MIN_VALUE;
    }

    if (RangeTo == 0)
    {
      tmpRT = Long.MAX_VALUE;
    }

    if (ValidityFrom == 0)
    {
      tmpVF = CommonConfig.LOW_DATE;
    }

    if (ValidityTo == 0)
    {
      tmpVT = CommonConfig.HIGH_DATE;
    }

    // Get/Create the group cache
    if (GroupCache.containsKey(Group))
    {
      tmpRangeItem = GroupCache.get(Group);

      // now run down the ranges until we find the right position
      while (tmpRangeItem != null)
      {
        tmpRangeNextNode = tmpRangeItem.nextRange;

        if ((tmpRF > tmpRangeItem.RangeFrom) &
            (tmpRangeNextNode == null))
        {
          // insert at the tail of the list if we are able
          newRangeItem = new RangeItem();
          newRangeItem.RangeFrom = tmpRF;
          newRangeItem.RangeTo = tmpRT;
          newRangeItem.ValidityFrom = tmpVF;
          newRangeItem.ValidityTo = tmpVT;
          newRangeItem.Results = Results;

          // Link to the previous range
          tmpRangeItem.nextRange = newRangeItem;

          // done
          return;
        }
        else if (tmpRF < tmpRangeItem.RangeFrom)
        {
          // insert at the head/middle of the list
          newRangeItem = new RangeItem();
          newRangeItem.RangeFrom = tmpRF;
          newRangeItem.RangeTo = tmpRT;
          newRangeItem.ValidityFrom = tmpVF;
          newRangeItem.ValidityTo = tmpVT;
          newRangeItem.Results = Results;
          newRangeItem.nextRange = tmpRangeItem.nextRange;
          tmpRangeItem.nextRange = newRangeItem;

          // done
          return;
        }

        // Move down the map
        tmpRangeItem = tmpRangeItem.nextRange;
      }

      // If we get here, we could not insert the period
      message = "Range From <" + RangeFrom +
              "> to <" + RangeTo + "> overlaps with another range in group <" +
              Group + ">";
      throw new InitializationException(message,getSymbolicName());
    }
    else
    {
      // create the new group and initialise
      tmpRangeItem = new RangeItem();
      tmpRangeItem.RangeFrom = tmpRF;
      tmpRangeItem.RangeTo = tmpRT;
      tmpRangeItem.ValidityFrom = tmpVF;
      tmpRangeItem.ValidityTo = tmpVT;
      tmpRangeItem.Results = Results;
      GroupCache.put(Group, tmpRangeItem);
    }
  }

 /**
  * Get an object from the Cache, using the number and the date
  *
  * @param Group The group to search
  * @param rangeSearchValue The value to search for
  * @param UTCDate The date to search for
  * @return The return results
  */
  public ArrayList<String> getEntryWithChildData(String Group, long rangeSearchValue, long UTCDate)
  {
    RangeItem tmpRangeItem;
    ArrayList<String> Value = null;

    // Get the Group
    tmpRangeItem  = GroupCache.get(Group);

    // search for the right object
    while (tmpRangeItem != null)
    {
      if ((tmpRangeItem.RangeFrom <= rangeSearchValue) &
          (tmpRangeItem.RangeTo >= rangeSearchValue))
      {
        // we have found a candidate - see if it is valid at the date
        if ((tmpRangeItem.ValidityFrom <= UTCDate) && (tmpRangeItem.ValidityTo > UTCDate))
        {
          // found it!
          return tmpRangeItem.Results;
        }
      }

      // Move down the map
      tmpRangeItem = tmpRangeItem.nextRange;
    }

    return Value;
  }

 /**
  * Get an object from the Cache, using the number and the date
  *
  * @param Group The group to search
  * @param rangeSearchValue The value to search for
  * @param UTCDate The date to search for
  * @return The return result
  */
  public String getEntry(String Group, long rangeSearchValue, long UTCDate)
  {
    RangeItem tmpRangeItem;

    // Get the Group
    tmpRangeItem  = GroupCache.get(Group);

    // search for the right object
    while (tmpRangeItem != null)
    {
      if ((tmpRangeItem.RangeFrom <= rangeSearchValue) &
          (tmpRangeItem.RangeTo >= rangeSearchValue))
      {
        // we have found a candidate - see if it is valid at the date
        if ((tmpRangeItem.ValidityFrom <= UTCDate) && (tmpRangeItem.ValidityTo > UTCDate))
        {
          // found it!
          return tmpRangeItem.Results.get(0);
        }
      }

      // Move down the map
      tmpRangeItem = tmpRangeItem.nextRange;
    }

    return NO_RANGE_MATCH;
  }

 /**
  * Load the data from the defined file
  *
  * @throws InitializationException
  */
  @Override
  public void loadDataFromFile()
                        throws InitializationException
  {
    // Variable declarations
    int            ObjectLinesLoaded = 0;
    BufferedReader inFile;
    String         tmpFileRecord;
    String[]       ObjectSplitFields;
    int            Index;
    int            tmpFieldCount = 0;
    String         tmpGroup;
    long           tmpRangeFrom;
    long           tmpRangeTo;
    long           tmpValidityFrom;
    long           tmpValidityTo;

    // Try to open the file
    try
    {
      inFile = new BufferedReader(new FileReader(cacheDataFile));
    }
    catch (FileNotFoundException ex)
    {
      message = "Application is not able to read file : <" +
            cacheDataFile + ">";
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
          ObjectLinesLoaded++;
          ObjectSplitFields = tmpFileRecord.split(";");

          // Set/Check the field count
          if (tmpFieldCount == 0)
          {
            // set it
            tmpFieldCount = ObjectSplitFields.length;

            // Check that it is valid - we cannot accept less than 4 fields
            if(tmpFieldCount < 4)
            {
              message = "The data must have >= 4 fields. We got <" + tmpFieldCount +
                                                "> fields in this record <" +
                                                tmpFileRecord + ">";
              throw new InitializationException(message,getSymbolicName());
            }
          }
          else
          {
            // Check that we remain consistent
            if (ObjectSplitFields.length != tmpFieldCount)
            {
              message = "The data must have <" + tmpFieldCount +
                                                "> fields. This record <" +
                                                tmpFileRecord + "> does not conform.";
              throw new InitializationException(message,getSymbolicName());
            }
          }

          // parse the input
          tmpGroup = ObjectSplitFields[0];
          tmpRangeFrom = Long.parseLong(ObjectSplitFields[1]);
          tmpRangeTo = Long.parseLong(ObjectSplitFields[2]);
          tmpValidityFrom = Long.parseLong(ObjectSplitFields[3]);
          tmpValidityTo = Long.parseLong(ObjectSplitFields[4]);

          ArrayList<String> tmpResults = new ArrayList<>();
          for (Index = 5 ; Index < ObjectSplitFields.length ; Index++)
          {
            tmpResults.add(ObjectSplitFields[Index]);
          }

          // Add into the cache
          addEntry(tmpGroup,tmpRangeFrom,tmpRangeTo,tmpValidityFrom,tmpValidityTo,tmpResults);

          // Update to the log file
          if ((ObjectLinesLoaded % loadingLogNotificationStep) == 0)
          {
            message = "Number Range Data Loading: <" + ObjectLinesLoaded +
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
            "> in record <" + ObjectLinesLoaded + ">. IO Error.");
    }
    catch (ArrayIndexOutOfBoundsException ex)
    {
      OpenRate.getOpenRateFrameworkLog().fatal(
            "Error reading input file <" + cacheDataFile +
            "> in record <" + ObjectLinesLoaded + ">. Malformed Record.");
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
          "Number Range Data Loading completed. <" + ObjectLinesLoaded +
          "> configuration lines loaded from <" +
          cacheDataFile + ">");
  }

 /**
  * Load the data from the defined Data Source
  */
  @Override
  public void loadDataFromDB()
                      throws InitializationException
  {
    int               Index;
    ResultSetMetaData Rsmd;
    int               ColumnCount;
    int               ObjectLinesLoaded = 0;
    String            tmpGroup;
    long              tmpRangeFrom;
    long              tmpRangeTo;
    long              tmpValidityFrom;
    long              tmpValidityTo;

    // Find the location of the  zone configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Zone Cache Loading from DB");

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
      message = "Error performing SQL for retieving Indexed Matchdata";
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // loop through the results for the customer login cache
    try
    {
      Rsmd = mrs.getMetaData();
      ColumnCount = Rsmd.getColumnCount();

      mrs.beforeFirst();

      while (mrs.next())
      {
        ObjectLinesLoaded++;
        tmpGroup = mrs.getString(1);
        tmpRangeFrom = mrs.getLong(2);
        tmpRangeTo = mrs.getLong(3);
        tmpValidityFrom = mrs.getLong(4);
        tmpValidityTo = mrs.getLong(5);

        ArrayList<String> tmpResults = new ArrayList<>();
        for (Index = 6 ; Index <= ColumnCount ; Index++)
        {
          tmpResults.add(mrs.getString(Index));
        }

        // Add into the cache
        addEntry(tmpGroup,tmpRangeFrom,tmpRangeTo,tmpValidityFrom,tmpValidityTo,tmpResults);

        // Update to the log file
        if ((ObjectLinesLoaded % loadingLogNotificationStep) == 0)
        {
          message = "Number Range Data Loading: <" + ObjectLinesLoaded +
                "> configurations loaded for <" + getSymbolicName() + "> from <" +
                cacheDataSourceName + ">";
          OpenRate.getOpenRateFrameworkLog().info(message);
        }
      }
    }
    catch (SQLException ex)
    {
      message = "Error opening Search Map Data for <" +
            cacheDataSourceName + ">";
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
      throw new InitializationException(message,ex,getSymbolicName());
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Number Range Data Loading completed. <" + ObjectLinesLoaded +
          "> configuration lines loaded from <" +
          cacheDataSourceName + ">");
  }

 /**
  * Load the data from the defined Data Source Method
  */
  @Override
  public void loadDataFromMethod()
                      throws InitializationException
  {
    throw new InitializationException("Not implemented yet",getSymbolicName());
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
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_OBJECT_COUNT, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_GROUP_COUNT, ClientManager.PARAM_DYNAMIC);
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
    int         ResultCode = -1;
    Collection<String>  tmpGroups;
    Iterator<String>    GroupIter;
    String      tmpGroupName;
    int         Objects = 0;
    RangeItem   tmpRangeItem;

    // Return the number of objects in the cache
    if (Command.equalsIgnoreCase(SERVICE_GROUP_COUNT))
    {
      return Integer.toString(GroupCache.size());
    }

    // Return the number of objects in the cache
    if (Command.equalsIgnoreCase(SERVICE_OBJECT_COUNT))
    {
      tmpGroups = GroupCache.keySet();
      GroupIter = tmpGroups.iterator();

      while (GroupIter.hasNext())
      {
        tmpGroupName = GroupIter.next();

        // Count the elements in the group
        tmpRangeItem = GroupCache.get(tmpGroupName);
        while (tmpRangeItem != null)
        {
          Objects++;
          tmpRangeItem = tmpRangeItem.nextRange;
        }
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
