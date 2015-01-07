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
import OpenRate.record.TimePacket;
import OpenRate.utils.PropertyUtils;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * This class implements the time model that can evaluate the
 * date and time of the CDR to find the corresponding time segment
 * that the call was made in (peak/off-peak etc). The CDR lookup
 * enters the lookup with the name of the Logic to search the model, and then
 * use the date and time of the CDR to locate the time segment to be used.
 *
 * The day range used in this is the "Calendar" object, minus 1
 * i.e. 0 = Sunday
 * ...
 *      6 = Saturday
 *
 * The format of the data to be loaded into the cache is:
 *
 * Model:
 *   Model      The identifier of the model
 *   Day        The idenitifier of the day, mapped if necessary through the "day
 *              map"
 *   StartTime  The start time of the period, in the format HH:MM
 *   EndTime    The end time of the period, in the format HH:MM
 *   TimeResult The result of the mapping
 *
 * NOTE: that the end times should be defined EXCLUSIVE so that the last minute
 * of the day is 23:59 NOT 00:00
 *
 * @author i.sparkes
 */
public class TimeModelCache
        extends AbstractSyncLoaderCache
{
    // List of Services that this Client supports
    private final static String SERVICE_OBJECT_COUNT = "ObjectCount";
    private final static String SERVICE_GROUP_COUNT = "GroupCount";

    // Queries we will be using to get the data
    private String ModelSelectQuery;
    private String MappingSelectQuery;
    private String DayMappingSelectQuery;
    private PreparedStatement StmtModelSelectQuery;
    private PreparedStatement StmtMappingSelectQuery;
    private PreparedStatement StmtDayMappingSelectQuery;

    // this is the persistent result set that we use to incrementally get the records
    private ResultSet mrsa = null;
    private ResultSet mrsb = null;
    private ResultSet mrsc = null;

    // This is used to control non-default day mapping loading
    private boolean dayMapDefined = false;

   /**
    * this is the name of the method we are to use in the case that the method
    * data source has been defined for loading the day map
    */
    protected static String DayCacheMethodName = null;

   /**
    * this is the name of the method we are to use in the case that the method
    * data source has been defined for loading the time model map
    */
    protected static String MapCacheMethodName = null;

    /**
     * this is the name of the method we are to use in the case that the method
     * data source has been defined for loading the time model map
     */
    protected static String ModelCacheMethodName = null;

   /**
    * A TimeInterval is a part of a time model. A model is made up of a list
    * of intervals that cover the whole of the possible 24 hour x 7 days
    */
    private class TimeIntervalNode {

        int TimeFrom;
        int TimeTo;
        String Result = "NEW";
        TimeIntervalNode child = null;
    }

   /**
    * A TimeMap is the list of intervals that make up the whole map.
    */
    private class TimeMap {

        // The vectors for the individual days
        TimeIntervalNode[] Intervals;
    }

   /**
    * This holds all of the configurations that make up a time model.
    */
    private HashMap<String, TimeMap> TimeModelCache;

   /**
    * This is the cache for the model definitions - we enter with an indetifier
    * and this returns the time model to use
    */
    private HashMap<String, String> ModelCache;

   /**
    * This is the cache for the model definitions - we enter with an indetifier
    * and this returns the time model to use
    */
    private HashMap<String, String> DayCache;

    /**
     * The default return when there is no match
     */
    public static final String NO_TIME_MATCH = "NOMATCH";

   /** Constructor
    * Creates a new instance of the Time Model Cache. The Cache
    * contains all of the TimeModels and Days that are known to the
    * module. The lookup is performed within the time model and the
    * day, calculated from the CDR date retrieveing the match for the
    * day and model map. We can support a maximum of 200 Model/Days
    * at the moment.
    */
    public TimeModelCache()
    {
      super();

      // Initialise the cache objects
      TimeModelCache = new HashMap<>(200);
      ModelCache = new HashMap<>(100);
      DayCache = new HashMap<>(7);

      // Call to default days
      addDefaultDays();
    }

    // Adding Default Days
    private void addDefaultDays()
    {
      addDay("0", "0");
      addDay("1", "1");
      addDay("2", "2");
      addDay("3", "3");
      addDay("4", "4");
      addDay("5", "5");
      addDay("6", "6");
    }

   /**
    * Add the model mapping between the plan and the model. The plan is generally
    * a name, while the model is where the intervals are stored.
    *
    * @param Plan The name of the plan
    * @param Model The model that holds the intervals
    */
    public void addModel(String Plan, String Model)
    {
      ModelCache.put(Plan, Model);
    }

   /**
    * Add a day to the day mapping cache. This allows the days as represented in
    * the external database to have different idenitfiers to the representation
    * in the cache. Internally, the days are integers, running from 0 (Sunday)
    * to 6 (Saturday)
    *
    * @param name The name of the day to add to the map
    * @param value The value of the day value to add
    */
    public void addDay(String name, String value)
    {
      DayCache.put(name, value);
    }

   /**
    * Add a value into the TimeModelCache, defining the result
    * value that should be returned in the case of a match.
    * @param Model The time model to add the interval to
    * @param From The time interval
    * @param Day The day the interval covers
    * @param To The time from for the interval (hh:mm)
    * @param ZoneResult The time interval result
    * @throws InitializationException
    */
    public void addInterval(String Model, String Day, String From, String To,
            String ZoneResult)
      throws InitializationException
    {
      int tmpDay;
      TimeMap tmpTimeMap;
      String[] ZoneTimeBits;
      TimeIntervalNode tmpIntervalNode;

      // See if we already have the model entry for this model
      if (!TimeModelCache.containsKey(Model)) {
        // Create the new Model Object
        tmpTimeMap = new TimeMap();

        // Add to hash
        TimeModelCache.put(Model, tmpTimeMap);

        // Create the root nodes
        tmpTimeMap.Intervals = new TimeIntervalNode[7];
      } else {
        // get the existing one
        tmpTimeMap = TimeModelCache.get(Model);
      }

      // Now add the node
      tmpDay = Integer.parseInt(Day);
      tmpIntervalNode = tmpTimeMap.Intervals[tmpDay];

      if (tmpIntervalNode == null) {
        tmpIntervalNode = new TimeIntervalNode();
        tmpTimeMap.Intervals[tmpDay] = tmpIntervalNode;
      } else {
        // find the end of the list
        while (tmpIntervalNode.child != null) {
            tmpIntervalNode = tmpIntervalNode.child;
        }

        // Add a new child
        tmpIntervalNode.child = new TimeIntervalNode();
        tmpIntervalNode = tmpIntervalNode.child;
      }

      // prepare the time from and time to
      ZoneTimeBits = From.split(":");
      tmpIntervalNode.TimeFrom = (Integer.parseInt(ZoneTimeBits[0]) * 60) +
              (Integer.parseInt(ZoneTimeBits[1]));
      ZoneTimeBits = To.split(":");
      tmpIntervalNode.TimeTo = (Integer.parseInt(ZoneTimeBits[0]) * 60) +
              (Integer.parseInt(ZoneTimeBits[1]));

      // deal with the case of "00:00" as midnight
      if (tmpIntervalNode.TimeTo == 0)
      {
        // map it to 23:59
        tmpIntervalNode.TimeTo = 24*60 - 1;
      }

      // basic sanity checks
      if (tmpIntervalNode.TimeFrom > tmpIntervalNode.TimeTo)
      {
        message = "From time <" + From + "> is later than To time <" +
                          To + "> in model <" + Model + "> and day <" + Day +
                          "> in module <" +getSymbolicName() + ">" ;
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message,getSymbolicName());
      }
      tmpIntervalNode.Result = ZoneResult;
    }

 /**
  * Get a value from the TimeModelCache
  *
  * NO TIME SPLITTING
  *
  * @param Plan The time model name
  * @param Day The day to check
  * @param Time The time to check
  * @return The return value
  */
  public String getEntry(String Plan, int Day, int Time)
  {
    TimeIntervalNode tmpIntervalNode;
    String Model;
    int CDRMinute;
    TimeMap tmpTimeMap;
    boolean finished;

    // Find the correct Time Model for the plan
    Model = ModelCache.get(Plan);

    if (Model == null) {
      return NO_TIME_MATCH;
    }

    // Get the interval
    tmpTimeMap = TimeModelCache.get(Model);

    if (tmpTimeMap == null)
    {
      OpenRate.getOpenRateFrameworkLog().warning("TimeMap for model <" + Plan + "> and day <" + Day + "> is empty in <" + getSymbolicName() + ">");
      return NO_TIME_MATCH;
    }

    // Get the root node for the day
    if (Day < 0 | Day > 6)
    {
      OpenRate.getOpenRateFrameworkLog().warning("Day <" + Day + "> is outside of the valid range of 0 (Sunday) to 6 (Saturday)");
      return NO_TIME_MATCH;
    }

    tmpIntervalNode = tmpTimeMap.Intervals[Day];

    // Search through the Day Segments
    CDRMinute = (Time);
    finished = false;

    // search the nodes
    while (!finished)
    {
      if (tmpIntervalNode == null)
      {
        OpenRate.getOpenRateFrameworkLog().warning("TimeMap for model <" + Plan + "> and day <" + Day + "> is empty in <" + getSymbolicName() + ">");
        return NO_TIME_MATCH;
      }

      if (CDRMinute >= tmpIntervalNode.TimeFrom) {
        if (CDRMinute <= tmpIntervalNode.TimeTo) {
          return tmpIntervalNode.Result;
        }
      }

      if (tmpIntervalNode.child != null) {
        tmpIntervalNode = tmpIntervalNode.child;
      } else {
        // run out of segments
        finished = true;
      }
    }

    return NO_TIME_MATCH;
  }

 /**
  * Evaluate the time models impacted over a time range, returning the
  * result as a vector of time packets.
  *
  * TIME SPLITTING
  *
  * @param TimeModel The time model we are using
  * @param CDRStartDate The start date/time of the event
  * @param CDREndDate The end date/time of the event
  * @param gCal The calendar object to use
  * @return TimePackets The list of periods impacted
  */
  public ArrayList<TimePacket> getEntry(String TimeModel, long CDRStartDate, long CDREndDate, GregorianCalendar gCal)
  {
    int    TMStartDayOfWeek;
    int    TMStartTime;
    int    TMEndTime;
    int    TMStartSecond;
    int    TMEndSecond;
    int    TotalDuration;

    // get the total duration, used for calculating the splitting factor
    TotalDuration = (int) (CDREndDate - CDRStartDate);

    // if we have a 0 duration call, force a duration if 1 (we can't manage a
    // 0 duration call) Ticket #679460
    if (TotalDuration == 0)
    {
      TotalDuration = 1;
      CDREndDate += 1;
    }

    // initialise the counters and return values
    long   tmpStartDateCounter = CDRStartDate;
    long   tmpEndDateCounter;
    ArrayList<TimePacket> packets = new ArrayList<>(1);
    ArrayList<TimePacket> dayPackets;

    // get the first start of day period
    gCal.setTimeInMillis(CDRStartDate*1000);
    gCal.set(Calendar.HOUR_OF_DAY, 0);
    gCal.set(Calendar.MINUTE, 0);
    gCal.set(Calendar.SECOND, 0);
    tmpEndDateCounter = gCal.getTimeInMillis()/1000;

    // loop until we have covered the whole period
    while (tmpStartDateCounter < CDREndDate)
    {
      // round up to the next day boundary
      tmpEndDateCounter += 86400;

      // See if we have covered the period
      if (tmpEndDateCounter > CDREndDate)
      {
        // yes, so round to the real end date
        tmpEndDateCounter = CDREndDate;

        // Get the prepared end date
        gCal.setTimeInMillis(tmpEndDateCounter*1000);
        TMEndTime = gCal.get(Calendar.HOUR_OF_DAY)*60;
        TMEndTime += gCal.get(Calendar.MINUTE);
        TMEndSecond = gCal.get(Calendar.SECOND);
      }
      else
      {
        // Get the default end of day prepared information
        TMEndTime = 24*60 - 1;
        TMEndSecond = 60;
      }

      // prepare the information for zoning
      gCal.setTimeInMillis(tmpStartDateCounter*1000);
      TMStartDayOfWeek = gCal.get(Calendar.DAY_OF_WEEK) - 1;
      TMStartTime = gCal.get(Calendar.HOUR_OF_DAY)*60;
      TMStartTime += gCal.get(Calendar.MINUTE);
      TMStartSecond = gCal.get(Calendar.SECOND);

      // Calculate the day based on the parameters
      dayPackets = getDayEntry(TimeModel, TMStartDayOfWeek, TMStartTime, TMStartSecond, TMEndTime, TMEndSecond, TotalDuration);

      // add to the current list
      packets.addAll(dayPackets);
      dayPackets.clear();

      // move on
      tmpStartDateCounter = tmpEndDateCounter;
    }

    // return the result
    return packets;
  }

 /**
  * Get the time packets for the given day
  *
  * @param TimeModel The time model to evaluate for
  * @param DayofWeek The day of the week we are working on
  * @param StartTime The start time to evaluate for
  * @param StartSecond The start second to evaluate for
  * @param EndTime The end time to evaluate for
  * @param EndSecond The end second to evaluate for
  * @param TotalDuration The original duration of the call
  * @return The time packets in the the given day
  */
  private ArrayList<TimePacket> getDayEntry(String TimeModel, int DayofWeek, int StartTime, int StartSecond, int EndTime, int EndSecond, int TotalDuration)
  {
      TimeIntervalNode tmpNode;
      ArrayList<TimePacket> packets = new ArrayList<>(1);
      int tmpStartTime;
      int tmpEndTime;
      int tmpStartSecond;

      tmpStartTime = StartTime;
      tmpStartSecond = StartSecond;
      tmpEndTime = EndTime;

      do
      {
        tmpNode = getEntryWithNode(TimeModel, DayofWeek, tmpStartTime);

        if (tmpNode != null)
        {
          if (tmpNode.TimeTo == 24*60 - 1)
          {
            if (EndTime <= tmpNode.TimeTo)
            {
              // last packet for the day - create and we have finished
              tmpEndTime = EndTime;
              CreateTimePacket(packets,DayofWeek,tmpStartTime,tmpStartSecond,tmpEndTime,EndSecond,TimeModel,tmpNode.Result,TotalDuration);
              break;
            }
            else
            {
              // normal packet
              tmpEndTime = tmpNode.TimeTo;
              CreateTimePacket(packets,DayofWeek,tmpStartTime,tmpStartSecond,tmpEndTime,EndSecond,TimeModel,tmpNode.Result,TotalDuration);

              // update the temp variables
              tmpStartTime = tmpNode.TimeTo;
              tmpEndTime = EndTime;
              tmpStartSecond = 0;
            }
          }
          else
          {
            if (tmpEndTime <= tmpNode.TimeTo)
            {
              // this covers the remaining time
              CreateTimePacket(packets,DayofWeek,tmpStartTime,tmpStartSecond,tmpEndTime,EndSecond,TimeModel,tmpNode.Result,TotalDuration);
              break;
            }
            else
            {
              // normal packet, and there is more to do
              CreateTimePacket(packets,DayofWeek,tmpStartTime,tmpStartSecond,tmpNode.TimeTo,60,TimeModel,tmpNode.Result,TotalDuration);

              // Update the variables
              tmpStartTime = tmpNode.TimeTo + 1;
              tmpEndTime = EndTime;
              tmpStartSecond = 0;
            }
          }
        }
      } while (tmpNode != null);

      return packets;
  }

 /**
  * Create a new time packet and fill it accordingly. This does no calculation
  * just instead creates the packet and adds it to the packet list
  *
  * @param packetList The list object to hold the time packets
  * @param Day The day
  * @param StartTime The start time
  * @param StartSecond The start
  * @param EndTime The end time
  * @param EndSecond The end second
  * @param TimeModel The time model
  * @param TimeResult The time result
  */
  private void CreateTimePacket(ArrayList<TimePacket> packetList,
                           int Day,
                           int StartTime,
                           int StartSecond,
                           int EndTime,
                           int EndSecond,
                           String TimeModel,
                           String TimeResult,
                           int TotalDuration)
  {
    TimePacket tmpPacket;

    tmpPacket = new TimePacket();
    tmpPacket.DayofWeek = Day;
    tmpPacket.StartTime = StartTime;
    tmpPacket.StartSecond = StartSecond;
    tmpPacket.EndTime = EndTime;
    tmpPacket.EndSecond = EndSecond;
    tmpPacket.TimeModel = TimeModel;
    tmpPacket.TimeResult = TimeResult;
    tmpPacket.TotalDuration = TotalDuration;

    // calculate the duration
    tmpPacket.Duration = (EndTime - StartTime)*60 - StartSecond + EndSecond;

    packetList.add(tmpPacket);
  }

 /**
  * Get a value from the TimeModelCache
  */
  private TimeIntervalNode getEntryWithNode(String Plan, int Day, int Time)
  {
      TimeIntervalNode tmpIntervalNode;
      String Model;
      int CDRMinute;
      TimeMap tmpTimeMap;
      boolean finished;

      // Find the correct Time Model for the plan
      Model = ModelCache.get(Plan);

      if (Model == null)
      {
          return null;
      }

      // Get the interval
      tmpTimeMap = TimeModelCache.get(Model);

      if (tmpTimeMap == null)
      {
        OpenRate.getOpenRateFrameworkLog().warning("TimeMap for model <" + Plan + "> and day <" + Day + "> is empty in <" + getSymbolicName() + ">");
        return null;
      }

      // Get the root node for the day
      if (Day < 0 | Day > 6)
      {
        OpenRate.getOpenRateFrameworkLog().warning("Day <" + Day + "> is outside of the valid range of 0 (Sunday) to 6 (Saturday)");
        return null;
      }

      tmpIntervalNode = tmpTimeMap.Intervals[Day];

      // Search through the Day Segments
      CDRMinute = (Time);
      finished = false;

      // search the nodes
      while (!finished)
      {
          if (tmpIntervalNode == null)
          {
            OpenRate.getOpenRateFrameworkLog().warning("TimeMap for model <" + Plan + "> and day <" + Day + "> is empty in <" + getSymbolicName() + ">");
            return null;
          }

          if (CDRMinute >= tmpIntervalNode.TimeFrom)
          {
              if (CDRMinute <= tmpIntervalNode.TimeTo)
              {
                  return tmpIntervalNode;
              }
          }

          if (tmpIntervalNode.child != null)
          {
              tmpIntervalNode = tmpIntervalNode.child;
          }
          else
          {
              // run out of segments
              finished = true;
          }
      }

      return null;
  }

// -----------------------------------------------------------------------------
// ------------------ Start of inherited Plug In functions ---------------------
// -----------------------------------------------------------------------------

 /**
  * loadDataFromFile is called automatically on startup of the
  * cache factory, as a result of implementing the CacheLoader
  * interface. This will be called if the data source is defined as "File"
  *
  * @throws InitializationException
  */
@Override
  public void loadDataFromFile()
          throws InitializationException {
      // Variable declarations
      int ModelsLoaded = 0;
      //int            TimeModelsLoaded = 0;
      int IntervalsLoaded = 0;
      int LinesLoaded = 0;
      BufferedReader inFile;
      String tmpFileRecord;
      String[] ZoneFields;

      // Find the location of the  zone configuration file
      OpenRate.getOpenRateFrameworkLog().info("Starting Time Cache Loading from File for cache <" + getSymbolicName() + ">");

      // Try to open the file
      try {
          inFile = new BufferedReader(new FileReader(cacheDataFile));
      } catch (FileNotFoundException fnfe) {
          message = "Not able to read time model data file <" +
                  cacheDataFile + "> in <" + getSymbolicName() + ">" ;
          OpenRate.getOpenRateFrameworkLog().error(message);
          throw new InitializationException(message,getSymbolicName());
      }

      // File open, now get the stuff
      try {
          while (inFile.ready()) {
              tmpFileRecord = inFile.readLine();

              if (tmpFileRecord.startsWith("Model")) {
                  // Get the model name
                  ZoneFields = tmpFileRecord.split(";");
                  addInterval(ZoneFields[1], ZoneFields[2], ZoneFields[3],
                          ZoneFields[4], ZoneFields[5]);
                  IntervalsLoaded++;
                  LinesLoaded++;
              }

              if (tmpFileRecord.startsWith("Mapping")) {
                  // Get the model name
                  ZoneFields = tmpFileRecord.split(";");
                  addModel(ZoneFields[1], ZoneFields[2]);
                  ModelsLoaded++;
                  LinesLoaded++;
              }
          }
      } catch (IOException ioe) {
          message = "Error reading input file in <" + getSymbolicName() +
                  "> at record <" + LinesLoaded + ">. IO Error message = <" +
                  ioe.getMessage() + ">";
          OpenRate.getOpenRateFrameworkLog().fatal(message);
          throw new InitializationException(message,getSymbolicName());
      } catch (ArrayIndexOutOfBoundsException aiobe) {
          message = "Error reading input file in <" + getSymbolicName() +
                  "> at record <" + LinesLoaded + ">. Malformed Record.";
          OpenRate.getOpenRateFrameworkLog().fatal(message);
      } finally {
          try {
              inFile.close();
          } catch (IOException ioe) {
            message = "Error closing input file in <" + getSymbolicName() +
                    ">. IO Error message = <" + ioe.getMessage() + ">";
            OpenRate.getOpenRateFrameworkLog().fatal(message);
          }
      }

      OpenRate.getOpenRateFrameworkLog().info("Time Model Cache: <" + IntervalsLoaded + "> Model intervals Loaded");
      OpenRate.getOpenRateFrameworkLog().info("Time Model Cache: <" + ModelsLoaded + "> Mappings Loaded");
      OpenRate.getOpenRateFrameworkLog().info(
              "Time Model Data Loading completed. <" + LinesLoaded +
          "> configuration lines loaded for <" + getSymbolicName() + " > from <"
          + cacheDataFile + ">");
  }

 /**
  * Load the data from the defined Data Source
  *
  * @throws InitializationException
  */
@Override
  public void loadDataFromDB()
          throws InitializationException
  {
    String Plan;
    int ModelsLoaded = 0;
    int IntervalsLoaded = 0;
    int DaysLoaded = 0;
    String To;
    String From;
    String Day;
    String Model;
    String Result;
    String Name;
    String Value;

    // Find the location of the  zone configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Time Model Cache Loading from DB for <" + getSymbolicName() + ">");

    // Try to open the DS
    JDBCcon = DBUtil.getConnection(cacheDataSourceName);

    // Now prepare the statements
    prepareStatements();

    // *** Day Mappings - Only perform if the statement has been defined ***
    if (dayMapDefined)
    {
        // clear the default cache first
        DayCache.clear();
        // Execute the day mapping query
        try {
            mrsc = StmtDayMappingSelectQuery.executeQuery();
        } catch (SQLException Sex) {
          message = "Error performing SQL for retieving day map data in <" + getSymbolicName() + ">. message = <" + Sex.getMessage() + ">";
          OpenRate.getOpenRateFrameworkLog().fatal(message);
          throw new InitializationException(message,getSymbolicName());
        }

        // loop through the results for the mapping entries
        try {
            mrsc.beforeFirst();

            while (mrsc.next()) {
                DaysLoaded++;
                Name = mrsc.getString(1);
                Value = mrsc.getString(2);
                addDay(Name, Value);
            }
        } catch (SQLException Sex) {
          message = "Error performing SQL for retieving day map data in <" + getSymbolicName() + ">. message = <" + Sex.getMessage() + ">";
          OpenRate.getOpenRateFrameworkLog().fatal(message);
          throw new InitializationException(message,getSymbolicName());
        }

        // Close down stuff
        try {
            mrsc.close();
            StmtDayMappingSelectQuery.close();
        } catch (SQLException Sex) {
          message = "Error closing day map data result set in <" + getSymbolicName() + ">. message = <" + Sex.getMessage() + ">";
          OpenRate.getOpenRateFrameworkLog().fatal(message);
          throw new InitializationException(message,getSymbolicName());
        }
    }

    // *** Models ***
    try {
        mrsa = StmtModelSelectQuery.executeQuery();
    } catch (SQLException Sex) {
        message = "Error performing SQL for retieving time map data in <" +getSymbolicName() + ">. message = <" + Sex.getMessage() + ">";
        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message,getSymbolicName());
    }

    // loop through the results for the model entries
    try {
        mrsa.beforeFirst();

        while (mrsa.next()) {
            IntervalsLoaded++;
            Model = mrsa.getString(1);
            Day = DayCache.get(mrsa.getString(2));

            // There was no mapping
            if (Day == null)
            {
              message = "Error reading Map Data for <" + getSymbolicName() + ">. No day map found for day value <" + mrsa.getString(2) + ">";
              OpenRate.getOpenRateFrameworkLog().fatal(message);
              throw new InitializationException(message,getSymbolicName());
            }

            From = mrsa.getString(3);
            To = mrsa.getString(4);
            Result = mrsa.getString(5);

            addInterval(Model, Day, From, To, Result);
        }
    } catch (SQLException Sex) {
        message = "SQL Error reading Map Data for <" + getSymbolicName() + ">. Messge = <" + Sex.getMessage() + ">";
        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message,getSymbolicName());
    }

    // Close down stuff
    try {
        mrsa.close();
        StmtModelSelectQuery.close();
    } catch (SQLException Sex) {
      message = "Error closing time map data result set in <" + getSymbolicName() + ">. message = <" + Sex.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // *** Mappings ***
    try {
      mrsb = StmtMappingSelectQuery.executeQuery();
    } catch (SQLException Sex) {
      message = "Error performing SQL for retieving time model data in <" +getSymbolicName() + ">. message = <" + Sex.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // loop through the results for the mapping entries
    try {
        mrsb.beforeFirst();

        while (mrsb.next()) {
            ModelsLoaded++;
            Plan = mrsb.getString(1);
            Model = mrsb.getString(2);

            addModel(Plan, Model);
        }
    } catch (SQLException Sex) {
      message = "SQL Error reading Model Data for <" + getSymbolicName() + ">. Messge = <" + Sex.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // Close down stuff
    DBUtil.close(mrsb);
    DBUtil.close(StmtMappingSelectQuery);

    // Close down the connection
    try {
        JDBCcon.close();
    } catch (SQLException Sex) {
      message = "Error closing time model data result set in <" + getSymbolicName() + ">. message = <" + Sex.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // check that we have complete coverage of the time models
    OpenRate.getOpenRateFrameworkLog().info("Time Model Cache: <" + IntervalsLoaded + "> Checking Model intervals");


    OpenRate.getOpenRateFrameworkLog().info("Time Model Cache: <" + IntervalsLoaded + "> Model intervals Loaded");
    OpenRate.getOpenRateFrameworkLog().info("Time Model Cache: <" + ModelsLoaded + "> Mappings Loaded");
    OpenRate.getOpenRateFrameworkLog().info("Time Model Cache: <" + DaysLoaded + "> Days Loaded");
    OpenRate.getOpenRateFrameworkLog().info("Time Model Data Loading completed from <" + cacheDataSourceName + ">");
  }

 /**
  * Load the data from the defined Data Source Method
  *
  * @throws InitializationException
  */
@Override
  public void loadDataFromMethod()
    throws InitializationException
  {
      String Plan;
      int ModelsLoaded = 0;
      int IntervalsLoaded = 0;
      int DaysLoaded = 0;
      String To;
      String From;
      String Day;
      String Model;
      String Result;
      String Name;
      String Value;
      ArrayList<String>    tmpMethodResult;

      // Find the location of the  zone configuration file
      OpenRate.getOpenRateFrameworkLog().info("Starting Time Model Cache Loading from Method for <" + getSymbolicName() + ">");

      // Execute the user domain method
      Collection<ArrayList<String>> methodLoadResultSet;

      if (dayMapDefined)
      {
        methodLoadResultSet = getMethodData(getSymbolicName(),DayCacheMethodName);

        if (methodLoadResultSet == null)
        {
          OpenRate.getOpenRateFrameworkLog().debug("No day map data returned by method <" + DayCacheMethodName +
                        "> in cache <" + getSymbolicName() + ">");
        }
        else
        {
          Iterator<ArrayList<String>> methodDataToLoadIterator = methodLoadResultSet.iterator();

          // clear the default cache first
          DayCache.clear();

          while (methodDataToLoadIterator.hasNext())
          {
            tmpMethodResult = methodDataToLoadIterator.next();

            DaysLoaded++;

            Name = tmpMethodResult.get(0);
            Value = tmpMethodResult.get(1);
            addDay(Name, Value);
          }
        }
      }

      // Now load the model intervals
      methodLoadResultSet = getMethodData(getSymbolicName(),MapCacheMethodName);

      if (methodLoadResultSet == null)
      {
        OpenRate.getOpenRateFrameworkLog().warning("No model map data returned by method <" + MapCacheMethodName +
                      "> in cache <" + getSymbolicName() + ">");
      }
      else
      {
        Iterator<ArrayList<String>> methodDataToLoadIterator = methodLoadResultSet.iterator();

        while (methodDataToLoadIterator.hasNext())
        {
          tmpMethodResult = methodDataToLoadIterator.next();

          IntervalsLoaded++;
          Model = tmpMethodResult.get(0);
          Day = DayCache.get(tmpMethodResult.get(1));

          // There was no mapping
          if (Day == null)
          {
            message = "Error reading Map Data for <" + getSymbolicName() + ">. No day map found for day value <" + tmpMethodResult.get(1) + ">";
            OpenRate.getOpenRateFrameworkLog().fatal(message);
            throw new InitializationException(message,getSymbolicName());
          }

          From = tmpMethodResult.get(2);
          To = tmpMethodResult.get(3);
          Result = tmpMethodResult.get(4);

          addInterval(Model, Day, From, To, Result);
        }
      }

      // Now load the model map
      methodLoadResultSet = getMethodData(getSymbolicName(),ModelCacheMethodName);

      if (methodLoadResultSet == null)
      {
        OpenRate.getOpenRateFrameworkLog().warning("No model map data returned by method <" + ModelCacheMethodName +
                      "> in cache <" + getSymbolicName() + ">");
      }
      else
      {
        Iterator<ArrayList<String>> methodDataToLoadIterator = methodLoadResultSet.iterator();

        while (methodDataToLoadIterator.hasNext())
        {
          tmpMethodResult = methodDataToLoadIterator.next();

          ModelsLoaded++;
          Plan = tmpMethodResult.get(0);
          Model = tmpMethodResult.get(1);

          addModel(Plan, Model);
        }
      }

      OpenRate.getOpenRateFrameworkLog().info("Time Model Cache: <" + IntervalsLoaded + "> Model intervals Loaded");
      OpenRate.getOpenRateFrameworkLog().info("Time Model Cache: <" + ModelsLoaded + "> Mappings Loaded");
      OpenRate.getOpenRateFrameworkLog().info("Time Model Cache: <" + DaysLoaded + "> Days Loaded");
      OpenRate.getOpenRateFrameworkLog().info("Time Model Data Loading completed from <" + cacheDataSourceName + ">");
  }

 /**
  * Clear down the cache contents in the case that we are ordered to reload
  */
  @Override
  public void clearCacheObjects()
  {
    TimeModelCache.clear();
    ModelCache.clear();

    if (dayMapDefined)
    {
      // clear it because we have a non-default map to load
      DayCache.clear();
    }
  }

  // -----------------------------------------------------------------------------
  // ---------------- Start of data base data layer functions --------------------
  // -----------------------------------------------------------------------------

 /**
  * get the select statement(s). Implemented as a separate function so that it can
  * be overwritten in implementation classes. By default the cache picks up the
  * statement with the name "SelectStatement".
  *
  * @param ResourceName The name of the resource to load for
  * @param CacheName The name of the cache to load for
  * @return True if the statements were found, otherwise false
  * @throws InitializationException
  */
  @Override
  protected boolean getDataStatements(String ResourceName, String CacheName) throws InitializationException {

      // Get the Select statement

      ModelSelectQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
              CacheName,
              "ModelSelectStatement",
              "None");

      MappingSelectQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
              CacheName,
              "MappingSelectStatement",
              "None");

      DayMappingSelectQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
              CacheName,
              "DaySelectStatement",
              "None");

      // if we have a day mapping defined, note it
      if (DayMappingSelectQuery.equalsIgnoreCase("None") == false)
      {
        dayMapDefined = true;
      }

      if (ModelSelectQuery.equals("None") | MappingSelectQuery.equals("None")) {
          return false;
      } else {
          return true;
      }
  }

 /**
  * get the data method to use. Implemented as a separate function so that it can
  * be overwritten in implementation classes. By default the cache picks up the
  * statement with the name "MethodName".
  *
  * @param ResourceName The name of the resource to load for
  * @param CacheName The name of the cache to load for
  * @return True if the statements were found, otherwise false
  * @throws InitializationException
  */
  @Override
  protected boolean getDataMethods(String ResourceName, String CacheName) throws InitializationException {

      // Get the Select statement

      ModelCacheMethodName = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
              CacheName,
              "ModelMethodName",
              "None");

      MapCacheMethodName = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
              CacheName,
              "MapMethodName",
              "None");

      DayCacheMethodName = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
              CacheName,
              "DayMethodName",
              "None");

      // if we have a day mapping defined, note it
      if (DayCacheMethodName.equalsIgnoreCase("None") == false)
      {
        dayMapDefined = true;
      }

      if (ModelCacheMethodName.equals("None") | MapCacheMethodName.equals("None")) {
          return false;
      } else {
          return true;
      }
  }

 /**
  * PrepareStatements creates the statements from the SQL expressions
  * so that they can be run as needed.
  *
  * @throws InitializationException
  */
  @Override
  protected void prepareStatements()
          throws InitializationException {
      try {
          // prepare the SQL for the TestStatement
          StmtModelSelectQuery = JDBCcon.prepareStatement(ModelSelectQuery,
                  ResultSet.TYPE_SCROLL_INSENSITIVE,
                  ResultSet.CONCUR_READ_ONLY);
      } catch (SQLException ex) {
          message = "Error preparing the statement " + ModelSelectQuery;
          OpenRate.getOpenRateFrameworkLog().error(message);
          throw new InitializationException(message,ex,getSymbolicName());
      }

      try {
          // prepare the SQL for the TestStatement
          StmtMappingSelectQuery = JDBCcon.prepareStatement(MappingSelectQuery,
                  ResultSet.TYPE_SCROLL_INSENSITIVE,
                  ResultSet.CONCUR_READ_ONLY);
      } catch (SQLException ex) {
          message = "Error preparing the statement " + MappingSelectQuery;
          OpenRate.getOpenRateFrameworkLog().error(message);
          throw new InitializationException(message,ex,getSymbolicName());

      }

      if (dayMapDefined) {
          try {
              // prepare the SQL for the TestStatement
              StmtDayMappingSelectQuery = JDBCcon.prepareStatement(DayMappingSelectQuery,
                      ResultSet.TYPE_SCROLL_INSENSITIVE,
                      ResultSet.CONCUR_READ_ONLY);
          } catch (SQLException ex) {
            message = "Error preparing the statement " + DayMappingSelectQuery;
            OpenRate.getOpenRateFrameworkLog().error(message);
            throw new InitializationException(message,ex,getSymbolicName());
          }
      }
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
    int ResultCode = -1;

    // Return the number of objects in the cache
    if (Command.equalsIgnoreCase(SERVICE_GROUP_COUNT))
    {
      return Integer.toString(TimeModelCache.size());
    }

    if (ResultCode == 0)
    {
      OpenRate.getOpenRateFrameworkLog().debug(LogUtil.LogECICacheCommand(getSymbolicName(), Command, Parameter));

      return "OK";
    }
    else
    {
      return super.processControlEvent(Command, Init, Parameter);
    }
  }
}

