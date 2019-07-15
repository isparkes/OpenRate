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
 * resource, over time to a key.
 */
public class ValidityFromCache
        extends AbstractSyncLoaderCache {

  // This stores the index to all the groups.
  private final HashMap<String, HashMap<String, ValidityNode>> GroupCache;

  /**
   * A ValidityNode is a segment of validity of a resource. These are chained
   * together in a sorted linked list. The sorting is done at insertion time
   * into the list, meaning that lookups at run time can be optimised.
   */
  private class ValidityNode {

    long TimeFrom;
    ArrayList<String> Results = null;
    ValidityNode child = null;
  }

  // List of Services that this Client supports
  private final static String SERVICE_GROUP_COUNT = "GroupCount";
  private final static String SERVICE_OBJECT_COUNT = "ObjectCount";

  /**
   * The default return when there is no match
   */
  public static final String NO_VALIDITY_MATCH = "NOMATCH";

  /**
   * Creates a new instance of the group Cache. The group Cache contains all of
   * the Groups that are later cached. The lookup is therefore performed within
   * the group, retrieving the validity segment for that group, resource_id and
   * time.
   */
  public ValidityFromCache() {
    super();

    GroupCache = new HashMap<>(500000);
  }

  /**
   * Add a value into the Validity Segment Cache, defining the result value that
   * should be returned in the case of a match. The entries are ordered during
   * the loading in a linked list sorted by validity date. This makes the search
   * at run time easier.
   *
   * @param Group The data group to add the entry to
   * @param ResourceID The resourceID of the entry to add
   * @param StartTime The start time of the validity
   * @param Results The list of result values
   */
  public void addEntry(String Group, String ResourceID, long StartTime,
          ArrayList<String> Results) {
    HashMap<String, ValidityNode> tmpResourceCache;
    ValidityNode tmpValidityNode;
    ValidityNode newNode;

    // See if we already have the group cache for this Group
    if (!GroupCache.containsKey(Group)) {
      // Create the new resource cache
      tmpResourceCache = new HashMap<>(100);

      // Add it to the group cache
      GroupCache.put(Group, tmpResourceCache);
    } else {
      // Otherwise just get the existing object
      tmpResourceCache = GroupCache.get(Group);
    }

    // Now add the validity segment into the list
    if (!tmpResourceCache.containsKey(ResourceID)) {
      // Create the new list
      tmpValidityNode = new ValidityNode();
      tmpValidityNode.TimeFrom = StartTime;
      tmpValidityNode.Results = Results;
      tmpValidityNode.child = null;

      // Add in the new node
      tmpResourceCache.put(ResourceID, tmpValidityNode);
    } else {
      // Recover the validity map that there is
      tmpValidityNode = tmpResourceCache.get(ResourceID);

      // now run down the validity periods until we find the right position
      while (tmpValidityNode != null) {
        if (StartTime < tmpValidityNode.TimeFrom) {
          // insert before the current node
          newNode = new ValidityNode();
          newNode.child = tmpValidityNode;
          newNode.TimeFrom = StartTime;
          newNode.Results = Results;
          
          // Update the head pointer in the hashmap
          tmpResourceCache.put(ResourceID, newNode);

          // done
          return;
        } else if ((StartTime > tmpValidityNode.TimeFrom)
                & (tmpValidityNode.child == null)) {
          // insert at the tail of the list
          newNode = new ValidityNode();
          tmpValidityNode.child = newNode;
          newNode.TimeFrom = StartTime;
          newNode.Results = Results;

          // done
          return;
        }

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }

      // If we get here, we could not insert the period
      OpenRate.getOpenRateFrameworkLog().error("Cache <" + getSymbolicName()
              + "> could not insert <" + Group + ":" + ResourceID + ":"
              + StartTime + ":" + "> without overlap.");
    }
  }

  /**
   * Returns the entry matching the resourceID in the given group at the given
   * time
   *
   * @param Group The resource group to search in
   * @param ResourceID The resource identifier to search for
   * @param Time The time to search for
   * @return The retrieved value, or "NOMATCH" if none found
   */
  public String getValiditySegmentMatch(String Group, String ResourceID, long Time) {
    HashMap<String, ValidityNode> tmpResourceCache;
    ValidityNode tmpValidityNode;

    // Get the service if we know it
    tmpResourceCache = GroupCache.get(Group);

    if (tmpResourceCache != null) {
      tmpValidityNode = tmpResourceCache.get(ResourceID);

      // Now that we have the Validity Map, get the entry
      while (tmpValidityNode != null) {
        if (Time >= tmpValidityNode.TimeFrom) {
          if (tmpValidityNode.child == null) {
            // end of chain, return what we have
            return tmpValidityNode.Results.get(0);
          } else {
            // next in chain is not valid for out time
            if (tmpValidityNode.child.TimeFrom > Time) {
              return tmpValidityNode.Results.get(0);
            }
          }
        }

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }
    }

    return NO_VALIDITY_MATCH;
  }

  /**
   * Returns the vector matching the resourceID in the given group at the given
   * time
   *
   * @param Group The resource group to search in
   * @param ResourceID The resource identifier to search for
   * @param Time The time to search for
   * @return The retrieved value vector, or null if none found
   */
  public ArrayList<String> getValiditySegmentMatchWithChildData(String Group, String ResourceID, long Time) {
    HashMap<String, ValidityNode> tmpResourceCache;
    ValidityNode tmpValidityNode;
    ArrayList<String> Value = null;

    // Get the service if we know it
    tmpResourceCache = GroupCache.get(Group);

    if (tmpResourceCache != null) {
      tmpValidityNode = tmpResourceCache.get(ResourceID);

      // Now that we have the Validity Map, get the entry
      while (tmpValidityNode != null) {
        if (Time >= tmpValidityNode.TimeFrom) {
          if (tmpValidityNode.child == null) {
            // end of chain, return what we have
            return tmpValidityNode.Results;
          } else {
            // next in chain is not valid for out time
            if (tmpValidityNode.child.TimeFrom > Time) {
              return tmpValidityNode.Results;
            }
          }
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
   *
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void loadDataFromFile()
          throws InitializationException {
    // Variable declarations
    int ValidityPeriodsLoaded = 0;
    BufferedReader inFile;
    String tmpFileRecord;
    String[] ZoneFields;
    String Group;
    String ResourceID;
    long TimeFrom;
    ArrayList<String> Result;
    int Index;
    String tmpStartDate = null;

    // Find the location of the  zone configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Validity Segment Cache Loading from File");

    // Try to open the file
    try {
      inFile = new BufferedReader(new FileReader(cacheDataFile));
    } catch (FileNotFoundException ex) {
      message = "Application is not able to read file : <"
              + cacheDataSourceName + ">";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message, ex, getSymbolicName());
    }

    // File open, now get the stuff
    try {
      while (inFile.ready()) {
        tmpFileRecord = inFile.readLine();

        if ((tmpFileRecord.startsWith("#"))
                | tmpFileRecord.trim().equals("")) {
          // Comment line or whitespace line, ignore
        } else {
          ValidityPeriodsLoaded++;
          ZoneFields = tmpFileRecord.split(";");
          Group = ZoneFields[0];
          ResourceID = ZoneFields[1];
          tmpStartDate = ZoneFields[2];
          TimeFrom = (int) fieldInterpreter.convertInputDateToUTC(tmpStartDate);

          // Interpret 0 values
          if (TimeFrom == 0) {
            TimeFrom = CommonConfig.LOW_DATE;
          }

          // now make an ArrayList of the results
          Result = new ArrayList<>();
          for (Index = 4; Index < ZoneFields.length; Index++) {
            Result.add(ZoneFields[Index]);
          }

          // Interpret 0 values
          if (TimeFrom == 0) {
            TimeFrom = CommonConfig.LOW_DATE;
          }

          addEntry(Group, ResourceID, TimeFrom, Result);

          // Update to the log file
          if ((ValidityPeriodsLoaded % loadingLogNotificationStep) == 0) {
            message = "Validity Segment Map Data Loading: <" + ValidityPeriodsLoaded
                    + "> configurations loaded for <" + getSymbolicName() + "> from <"
                    + cacheDataFile + ">";
            OpenRate.getOpenRateFrameworkLog().info(message);
          }
        }
      }
    } catch (IOException ex) {
      OpenRate.getOpenRateFrameworkLog().fatal(
              "Error reading input file <" + cacheDataFile
              + "> in record <" + ValidityPeriodsLoaded + ">. IO Error.");
    } catch (ArrayIndexOutOfBoundsException ex) {
      OpenRate.getOpenRateFrameworkLog().fatal(
              "Error reading input file <" + cacheDataFile
              + "> in record <" + ValidityPeriodsLoaded + ">. Malformed Record.");
    } catch (ParseException pe) {
      message
              = "Error converting date from <" + getSymbolicName() + "> in record <"
              + ValidityPeriodsLoaded + ">. Unexpected date value <" + tmpStartDate
              + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    } finally {
      try {
        inFile.close();
      } catch (IOException ex) {
        OpenRate.getOpenRateFrameworkLog().error(
                "Error closing input file <" + cacheDataFile
                + ">", ex);
      }
    }

    OpenRate.getOpenRateFrameworkLog().info(
            "Validity Segment Map Data Loading completed. "
            + ValidityPeriodsLoaded + " configuration lines loaded from <"
            + cacheDataFile + ">");
  }

  /**
   * Load the data from the defined Data Source
   *
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void loadDataFromDB()
          throws InitializationException {
    int ValidityPeriodsLoaded = 0;
    String Group;
    String ResourceID;
    long TimeFrom;
    ArrayList<String> Result;
    int Index;
    String tmpStartDate = null;

    // Find the location of the  zone configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Validity Segment Cache Loading from DB in <" + getSymbolicName() + ">");

    // Try to open the DS
    JDBCcon = DBUtil.getConnection(cacheDataSourceName);

    // Now prepare the statements
    prepareStatements();

    // Execute the query
    try {
      mrs = StmtCacheDataSelectQuery.executeQuery();
    } catch (SQLException ex) {
      message = "Error performing SQL for retrieving Validity Segment Match data in <" + getSymbolicName() + ">. message = <" + ex.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // loop through the results for the customer login cache
    try {
      mrs.beforeFirst();

      while (mrs.next()) {
        ValidityPeriodsLoaded++;
        Group = mrs.getString(1);
        ResourceID = mrs.getString(2);
        tmpStartDate = mrs.getString(3);
        TimeFrom = (int) fieldInterpreter.convertInputDateToUTC(tmpStartDate);

        // Interpret 0 values
        if (TimeFrom == 0) {
          TimeFrom = CommonConfig.LOW_DATE;
        }

        // now make an ArrayList of the results
        Result = new ArrayList<>();
        for (Index = 4; Index <= mrs.getMetaData().getColumnCount(); Index++) {
          Result.add(mrs.getString(Index));
        }

        // Add the map
        addEntry(Group, ResourceID, TimeFrom, Result);

        // Update to the log file
        if ((ValidityPeriodsLoaded % loadingLogNotificationStep) == 0) {
          message = "Validity Segment Map Data Loading: <" + ValidityPeriodsLoaded
                  + "> configurations loaded for <" + getSymbolicName() + "> from <"
                  + cacheDataSourceName + ">";
          OpenRate.getOpenRateFrameworkLog().info(message);
        }
      }
    } catch (SQLException ex) {
      message = "Error opening Validity Segment Match Data for <" + getSymbolicName() + ">. message = <" + ex.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    } catch (ParseException pe) {
      message
              = "Error converting date from <" + getSymbolicName() + "> in record <"
              + ValidityPeriodsLoaded + ">. Unexpected date value <" + tmpStartDate
              + ">. message = <" + pe.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message, pe);
      throw new InitializationException(message, getSymbolicName());
    }

    // Close down stuff
    try {
      mrs.close();
      StmtCacheDataSelectQuery.close();
      JDBCcon.close();
    } catch (SQLException ex) {
      message = "Error closing Validity Segment Match Data for <" + getSymbolicName() + ">. message = <" + ex.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }

    OpenRate.getOpenRateFrameworkLog().info(
            "Validity Segment Map Data Loading completed. "
            + ValidityPeriodsLoaded + " configuration lines loaded from <"
            + cacheDataSourceName + ">");
  }

  /**
   * Load the data from the defined Data Source Method
   *
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void loadDataFromMethod()
          throws InitializationException {
    // Variable declarations
    int ValidityPeriodsLoaded = 0;
    int formFactor;
    String Group;
    String ResourceID;
    long TimeFrom;
    ArrayList<String> Result;
    int Index;
    ArrayList<String> tmpMethodResult;

    // Find the location of the  zone configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Validity Segment Cache Loading from Method for <" + getSymbolicName() + ">");

    // Execute the user domain method
    Collection<ArrayList<String>> methodLoadResultSet;

    methodLoadResultSet = getMethodData(getSymbolicName(), CacheMethodName);

    Iterator<ArrayList<String>> methodDataToLoadIterator = methodLoadResultSet.iterator();

    // loop through the results for the customer login cache
    while (methodDataToLoadIterator.hasNext()) {
      ValidityPeriodsLoaded++;
      tmpMethodResult = methodDataToLoadIterator.next();
      formFactor = tmpMethodResult.size();

      if (formFactor < 5) {
        // There are not enough fields
        message = "Error reading input data from <" + cacheDataSourceName
                + "> in record <" + ValidityPeriodsLoaded + ">. Not enough fields.";

        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message, getSymbolicName());
      }

      Group = tmpMethodResult.get(0);
      ResourceID = tmpMethodResult.get(1);
      TimeFrom = Integer.valueOf(tmpMethodResult.get(2));

      // Interpret 0 values
      if (TimeFrom == 0) {
        TimeFrom = CommonConfig.LOW_DATE;
      }

      // now make an ArrayList of the results
      Result = new ArrayList<>();
      for (Index = 4; Index < tmpMethodResult.size(); Index++) {
        Result.add(tmpMethodResult.get(Index));
      }

      addEntry(Group, ResourceID, TimeFrom, Result);

      // Update to the log file
      if ((ValidityPeriodsLoaded % loadingLogNotificationStep) == 0) {
        message = "Validity Segment Map Data Loading: <" + ValidityPeriodsLoaded
                + "> configurations loaded for <" + getSymbolicName() + "> from <"
                + cacheDataSourceName + ">";
        OpenRate.getOpenRateFrameworkLog().info(message);
      }
    }

    OpenRate.getOpenRateFrameworkLog().info(
            "Validity Segment Map Data Loading completed. "
            + ValidityPeriodsLoaded + " configuration lines loaded from <"
            + cacheDataSourceName + ">");
  }

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------
  /**
   * registerClientManager registers the client module to the ClientManager
   * class which manages all the client modules available in this OpenRate
   * Application.
   *
   * registerClientManager registers this class as a client of the ECI listener
   * and publishes the commands that the plug in understands. The listener is
   * responsible for delivering only these commands to the plug in.
   *
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void registerClientManager() throws InitializationException {
    // Set the client reference and the base services first
    super.registerClientManager();

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_GROUP_COUNT, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_OBJECT_COUNT, ClientManager.PARAM_DYNAMIC);
  }

  /**
   * processControlEvent is the method that will be called when an event is
   * received for a module that has registered itself as a client of the
   * External Control Interface
   *
   * @param Command - command that is understand by the client module
   * @param Init - we are performing initial configuration if true
   * @param Parameter - parameter for the command
   * @return The result string of the operation
   */
  @Override
  public String processControlEvent(String Command, boolean Init,
          String Parameter) {
    HashMap<String, ValidityNode> tmpResource;
    Collection<String> tmpGroups;
    Iterator<String> GroupIter;
    String tmpGroupName;
    int Objects = 0;
    int ResultCode = -1;

    // Return the number of objects in the cache
    if (Command.equalsIgnoreCase(SERVICE_GROUP_COUNT)) {
      return Integer.toString(GroupCache.size());
    }

    if (Command.equalsIgnoreCase(SERVICE_OBJECT_COUNT)) {
      tmpGroups = GroupCache.keySet();
      GroupIter = tmpGroups.iterator();

      while (GroupIter.hasNext()) {
        tmpGroupName = GroupIter.next();
        tmpResource = GroupCache.get(tmpGroupName);
        Objects += tmpResource.size();
      }

      return Integer.toString(Objects);
    }

    if (ResultCode == 0) {
      OpenRate.getOpenRateFrameworkLog().debug(LogUtil.LogECICacheCommand(getSymbolicName(), Command, Parameter));

      return "OK";
    } else {
      return super.processControlEvent(Command, Init, Parameter);
    }
  }

  /**
   * Clear down the cache contents in the case that we are ordered to reload
   */
  @Override
  public void clearCacheObjects() {
    GroupCache.clear();
  }
}
