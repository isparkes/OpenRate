
package OpenRate.cache;

import OpenRate.OpenRate;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.utils.PropertyUtils;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * This class extends the basic rating scheme found in the "RateCache" module to
 * implement a rating scheme based on full mappable RUM (rateable Usage Metric)
 * model for rating records according to a tier and beat model, as well as
 * impacting multiple resources using multiple RUMs.
 *
 * The data is read in from a configuration file or database, and contains two
 * distinct sorts of information: - RUM Map. This describes the price models to
 * apply, the RUMs to read and the resources to impact.
 *
 * For the data source type "File", the data for the RUM Map will be read from
 * the file that you give under "RUMMapDataFile".
 *
 * For the data source type "DB", the data for the RUM Map will be read from the
 * query that you give under "RUMMapStatement".
 *
 * In either case, the data that is read is:
 *
 * RUM Map --------------- PriceGroup;PriceModel;RUM;Resource;RUMType;ResCtr
 *
 * Where: 'PriceGroup' is a unique code for the Price Model Group. All price
 * models within the group will be executed 'PriceModel' is a unique code for
 * the PriceModel 'RUM' is the name of the RUM to be used by the price model
 * 'Resource' is the resource to impact 'RUMType' is the type of rating that
 * should be performed on the RUM, and can be one of the following: 'Flat' -
 * will return a simple RUM * Price, without tiers or beats 'Tiered' - will
 * evaluate all tiers, rating the portion of the RUM that lies within the tier
 * individually 'Threshold' - will rate all of the RUM in the tier that the RUM
 * lies in 'Event' - will return a fixed value regardless of the RUM value
 * 'ResCtr' is the counter to be impacted for this resource
 *
 * @author i.sparkes
 */
public class RUMMapCache
        extends AbstractSyncLoaderCache {

  /**
   * RUM Map entry
   */
  public class RUMMapEntry {

    /**
     * RUMType indicates the type of rating we are to use
     */
    public int RUMType;

    /**
     * The name of the price model
     */
    public String PriceModel;

    /**
     * The Rateable Usage Metric - indicates what value we are rating
     */
    public String RUM;

    /**
     * The resource that we are to impact
     */
    public String Resource;

    /**
     * The counter ID for the resource to impact
     */
    public int ResourceCounter;

    /**
     * If this is true, we reduce the amount of RUM to rate after rating
     */
    public boolean ConsumeRUM = false;
  }

  /**
   * This holds the RUM map
   */
  protected HashMap<String, ArrayList<RUMMapEntry>> RUMMapCache;

  /**
   * this is the name of the file that holds the RUM Map
   */
  protected static String RUMMapDataFile;

// -----------------------------------------------------------------------------
// ------------------ Start of inherited Plug In functions ---------------------
// -----------------------------------------------------------------------------
  /**
   * loadCache is called automatically on startup of the cache factory, as a
   * result of implementing the CacheLoader interface. This should be used to
   * load any data that needs loading, and to set up variables.
   *
   * @param ResourceName The name of the resource to load for
   * @param CacheName The name of the cache to load for
   * @throws InitializationException
   */
  @Override
  public void loadCache(String ResourceName, String CacheName)
          throws InitializationException {
    int initialObjectSize = 1000;

    String tmpInitOjectSize = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
            CacheName,
            "InitialObjectSize",
            "1000");
    try {
      initialObjectSize = Integer.parseInt(tmpInitOjectSize);
    } catch (NumberFormatException nfe) {
      message = "Could not parse initial object size <" + initialObjectSize
              + "> for cache <" + getSymbolicName() + ">";
      throw new InitializationException(message, getSymbolicName());
    }

    // inform the user about the start of the price model phase
    OpenRate.getOpenRateFrameworkLog().debug("Setting initial hash map size to <" + initialObjectSize + "> for cache <" + getSymbolicName() + ">");

    RUMMapCache = new HashMap<>(initialObjectSize);

    // Do the parent initialisation
    super.loadCache(ResourceName, CacheName);
  }

  // -----------------------------------------------------------------------------
  // ----------------------- Start of custom functions ---------------------------
  // -----------------------------------------------------------------------------
  /**
   * Add a value into the price map cache.
   *
   * @param PriceGroup The name of the price group to map
   * @param PriceModel The name of the price model to add to the map
   * @param RUM The name of the RUM to apply for this price model
   * @param Resource The name of the Resource to impact
   * @param RUMType The type of RUM for this impact
   * @param ResourceCounter The counter ID for the resource
   * @throws OpenRate.exception.InitializationException
   */
  public void addRUMMap(String PriceGroup, String PriceModel, String RUM, String Resource, String RUMType, String ResourceCounter) throws InitializationException {

    ArrayList<RUMMapEntry> tmpRUMMapCache;
    RUMMapEntry tmpRMEntry;

    // See if we already have the cache object for this price
    if (!RUMMapCache.containsKey(PriceGroup)) {

      // Create the new PriceModel object
      tmpRUMMapCache = new ArrayList<>();
      RUMMapCache.put(PriceGroup, tmpRUMMapCache);
      tmpRMEntry = new RUMMapEntry();
      tmpRMEntry.PriceModel = PriceModel;
      tmpRMEntry.RUM = RUM;
      tmpRMEntry.Resource = Resource;
      tmpRMEntry.ResourceCounter = Integer.parseInt(ResourceCounter);

      if (RUMType.equalsIgnoreCase("flat")) {
        tmpRMEntry.RUMType = 1;
      } else if (RUMType.equalsIgnoreCase("tiered")) {
        tmpRMEntry.RUMType = 2;
      } else if (RUMType.equalsIgnoreCase("threshold")) {
        tmpRMEntry.RUMType = 3;
      } else if (RUMType.equalsIgnoreCase("event")) {
        tmpRMEntry.RUMType = 4;
      } else {
        message = "Unknown rating type <" + RUMType + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message, getSymbolicName());
      }

      // so add the entry to the new map. No need to order it, it is the first
      tmpRUMMapCache.add(tmpRMEntry);
    } else {

      // Otherwise just add it to the existing rate model
      tmpRUMMapCache = RUMMapCache.get(PriceGroup);

      // Add the new entry
      tmpRMEntry = new RUMMapEntry();
      tmpRMEntry.PriceModel = PriceModel;
      tmpRMEntry.RUM = RUM;
      tmpRMEntry.Resource = Resource;
      tmpRMEntry.ResourceCounter = Integer.parseInt(ResourceCounter);

      if (RUMType.equalsIgnoreCase("flat")) {
        tmpRMEntry.RUMType = 1;
      } else if (RUMType.equalsIgnoreCase("tiered")) {
        tmpRMEntry.RUMType = 2;
      } else if (RUMType.equalsIgnoreCase("threshold")) {
        tmpRMEntry.RUMType = 3;
      } else if (RUMType.equalsIgnoreCase("event")) {
        tmpRMEntry.RUMType = 4;
      } else {
        message = "Unknown rating type <" + RUMType + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message, getSymbolicName());
      }

      // Add the object to the ArrayList
      tmpRUMMapCache.add(tmpRMEntry);
    }
  }

  /**
   * Get a value from the RateCache. The processing based on the result returned
   * here is evaluated in the twinned processing class, in order to reduce the
   * load on the main framework thread.
   *
   * @param key The identifier for the RUM map to recover
   * @return The RUM map containing all of the pricemodel-RUM-Resource
   * combinations
   */
  public ArrayList<RUMMapEntry> getRUMMap(String key) {

    ArrayList<RUMMapEntry> tmpEntry;

    // Get the rate plan
    tmpEntry = RUMMapCache.get(key);

    // and return it
    return tmpEntry;
  }

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited loading functions ---------------------
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
    int RatesLoaded = 0;
    BufferedReader inFile;
    String tmpFileRecord;
    String[] RateFields;

    int MapsLoaded = 0;
    String tmpGroup;
    String tmpModel;
    String tmpRUM;
    String tmpResource;
    String tmpRUMType;
    String tmpResCtr;

    // ****** perform the loading of the model descriptors ******
    // inform the user about the start of the price group phase
    OpenRate.getOpenRateFrameworkLog().info("Starting RUM Map Data Loading from file for <" + getSymbolicName() + ">");

    // Try to open the file
    try {
      inFile = new BufferedReader(new FileReader(cacheDataFile));
    } catch (FileNotFoundException fnfe) {
      message = "Not able to read file : <"
              + cacheDataFile + ">. message = <" + fnfe.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // File open, now get the stuff
    try {
      while (inFile.ready()) {
        tmpFileRecord = inFile.readLine();

        if ((tmpFileRecord.startsWith("#"))
                | tmpFileRecord.trim().equals("")) {
          // Comment line, ignore
        } else {
          MapsLoaded++;
          RateFields = tmpFileRecord.split(";");

          // Prepare and add the line
          tmpGroup = RateFields[1];
          tmpModel = RateFields[2];
          tmpRUM = RateFields[3];
          tmpResource = RateFields[4];
          tmpRUMType = RateFields[5];
          tmpResCtr = RateFields[6];

          addRUMMap(tmpGroup, tmpModel, tmpRUM, tmpResource, tmpRUMType, tmpResCtr);
        }
      }
    } catch (IOException ex) {
      message = "Error reading input file <" + cacheDataFile
              + "> in record <" + RatesLoaded + ">. IO Error.";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    } catch (ArrayIndexOutOfBoundsException ex) {
      message
              = "Error reading input file <" + cacheDataFile
              + "> in record <" + RatesLoaded + ">. Malformed Record.";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    } finally {
      try {
        inFile.close();
      } catch (IOException ex) {
        message = "Error closing input file <" + cacheDataFile
                + ">. message = <" + ex.getMessage() + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message, getSymbolicName());
      }
    }

    OpenRate.getOpenRateFrameworkLog().info(
            "Price Group Data Loading completed. " + MapsLoaded
            + " configuration lines loaded from <" + cacheDataFile
            + ">");
  }

  /**
   * Load the data from the defined Data Source
   *
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void loadDataFromDB()
          throws InitializationException {
    int MapsLoaded = 0;
    String tmpGroup = null;
    String tmpModel = null;
    String tmpRUM = null;
    String tmpResource = null;
    String tmpRUMType = null;
    String tmpResCtr = null;

    int columns;

    // ****** perform the loading of the model descriptors ******
    // Find the location of the configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Price Group Data Loading from DB for <" + getSymbolicName() + ">");

    // Try to open the DS
    JDBCcon = DBUtil.getConnection(cacheDataSourceName);

    // Now prepare the statements
    prepareStatements();

    // Execute the query
    try {
      mrs = StmtCacheDataSelectQuery.executeQuery();
      columns = mrs.getMetaData().getColumnCount();
    } catch (SQLException ex) {
      message = "Error performing SQL for retieving Price Group Data for <"
              + getSymbolicName() + ">. SQL Error = <" + ex.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // check we have something we can use - we expect 7 fields. Everything
    // else is BAD
    if (columns == 6) {
      // loop through the results for the customer login cache
      try {
        mrs.beforeFirst();

        while (mrs.next()) {
          MapsLoaded++;
          tmpGroup = mrs.getString(1);
          tmpModel = mrs.getString(2);
          tmpRUM = mrs.getString(3);
          tmpResource = mrs.getString(4);
          tmpRUMType = mrs.getString(5);
          tmpResCtr = mrs.getString(6);

          // Add the map
          addRUMMap(tmpGroup, tmpModel, tmpRUM, tmpResource, tmpRUMType, tmpResCtr);
        }
      } catch (SQLException Sex) {
        message = "Error opening Price Group Data for <"
                + getSymbolicName() + ">. SQL Error = <" + Sex.getMessage() + ">";
        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message, getSymbolicName());
      } catch (NullPointerException npe) {
        message = "Null value loading Price Group Data for <"
                + getSymbolicName() + ">. Group <" + tmpGroup
                + ">, Model <" + tmpModel + ">, RUM <" + tmpRUM
                + ">, Resource <" + tmpResource + ">, RUM Type <"
                + tmpRUMType + ">, Step <" + tmpResCtr + ">";
        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message, getSymbolicName());
      }
    } else {
      // Not a valid number of fields
      message = "Invalid number of fields in rum map loading for module <"
              + getSymbolicName() + ">. Expecting <6>, but got <" + columns + ">.";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // Close down stuff
    try {
      mrs.close();
      StmtCacheDataSelectQuery.close();
      JDBCcon.close();
    } catch (SQLException ex) {
      message = "Error closing Price Group Data connection for <"
              + getSymbolicName() + ">. SQL Error = <" + ex.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }

    OpenRate.getOpenRateFrameworkLog().info(
            "Price Group Data Loading completed. " + MapsLoaded
            + " configuration lines loaded from <" + getSymbolicName()
            + ">");
  }

  /**
   * Load the data from the defined Data Source Method
   *
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void loadDataFromMethod()
          throws InitializationException {
    throw new InitializationException("Not implemented yet", getSymbolicName());
  }

  /**
   * Clear down the cache contents in the case that we are ordered to reload
   */
  @Override
  public void clearCacheObjects() {
    // clear the RUM map cache
    RUMMapCache.clear();
  }
}
