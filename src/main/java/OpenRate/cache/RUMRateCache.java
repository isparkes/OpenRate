
package OpenRate.cache;

import OpenRate.CommonConfig;
import OpenRate.OpenRate;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.record.RateMapEntry;
import OpenRate.utils.PropertyUtils;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Please
 * <a target='new' href='http://www.open-rate.com/wiki/index.php?title=RUM_Rate_Cache'>click
 * here</a> to go to wiki page.
 * <br>
 * <p>
 * This class extends the basic rating scheme found in the "RateCache" module to
 * implement a rating scheme based on full mappable RUM (rateable Usage Metric)
 * model for rating records according to a tier and beat model, as well as
 * impacting multiple resources using multiple RUMs.
 *
 * The data is read in from a configuration file or database, and contains two
 * distinct sorts of information: - Price Model Map. This describes the price
 * models - RUM Map. This describes the price models to apply, the RUMs to read
 * and the resources to impact.
 *
 * For the data source type "File", the data for the Price Model Map will be
 * read from the file you give under "PriceModelDataFile". The RUM Map will be
 * read from the file that you give under "RUMMapDataFile".
 *
 * For the data source type "DB", the data for the Price Model Map will be read
 * from the query you give under "PriceModelStatement". The RUM Map will be read
 * from the query that you give under "RUMMapStatement".
 *
 * In either case, the data that is read is:
 *
 * Price Model Map ---------------
 * PriceModel;Step;TierFrom;TierTo;Beat;Factor;ChargeBase
 *
 * Where: 'PriceModel' is the identifier for a price model 'Step' is the number
 * of the tier, starting from 1 and incrementing for each tier to be evaluated
 * 'TierFrom' is the start of the tier, usually tiers will start from 1 'TierTo'
 * is the end of the tier 'Beat' is the granularity of the rating (should be an
 * exact fraction of the tier) 'Factor' is the cost of each "charge base" number
 * of units in this tier 'ChargeBase' is the number of units for which the cost
 * factor has been defined. This lets you define prices as minutes (ChargeBase =
 * 60), but still rate on a per second basis (Beat = 1). 'StartTime' is the
 * start of the validity of this step, allowing multiple validity periods for
 * each step. A value of 0 in this field means that the step will be valid for
 * ever 'EndTime' is the end of the validity of this step, allowing multiple
 * validity periods for each step
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
public class RUMRateCache
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
   * This stores all the cacheable data necessary for the definition of the rate
   * plans.
   */
  protected HashMap<String, ArrayList<RateMapEntry>> PriceModelCache;

  /**
   * This holds the RUM map
   */
  protected HashMap<String, ArrayList<RUMMapEntry>> RUMMapCache;

  /**
   * these are the statements that we have to prepare to be able to get records
   * once and only once
   */
  protected static String PriceModelDataSelectQuery;

  /**
   * these are the prepared statements
   */
  protected static PreparedStatement StmtPriceModelDataSelectQuery;

  /**
   * these are the statements that we have to prepare to be able to get records
   * once and only once
   */
  protected static String RUMMapDataSelectQuery;

  /**
   * these are the prepared statements
   */
  protected static PreparedStatement StmtRUMMapDataSelectQuery;

  /**
   * this is the name of the file that holds the RUM Map
   */
  protected static String RUMMapDataFile;

  /**
   * this is the name of the file that holds price models
   */
  protected static String PriceModelDataFile;

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

    PriceModelCache = new HashMap<>(initialObjectSize);
    RUMMapCache = new HashMap<>(initialObjectSize);

    // Do the parent initialisation
    super.loadCache(ResourceName, CacheName);
  }

  // -----------------------------------------------------------------------------
  // ----------------------- Start of custom functions ---------------------------
  // -----------------------------------------------------------------------------
  /**
   * Add a value into the RateCache, defining the RateMapEntry result value that
   * should be returned in the case of a match. A PriceModel is therefore
   * defined as a group of RateMapEntries, that make up a whole rate map.
   *
   * @param priceModel The price model name to add
   * @param step The tier number (starting from 1) to enable multiple tiers
   * @param from The start of the tier
   * @param to The end of the tier
   * @param beat The charging granularity
   * @param factor The value to charge for each beat
   * @param chargeBase The base amount to charge the factor for
   * @param startTime The start time of the validity
   * @throws InitializationException
   */
  public void addPriceModel(
          String priceModel,
          int step,
          double from,
          double to,
          double beat,
          double factor,
          double chargeBase,
          long startTime)
          throws InitializationException {

    ArrayList<RateMapEntry> tmpRateCache;
    RateMapEntry tmpRMEntry;
    RateMapEntry helperRMEntry;
    int i;
    boolean inserted = false;

    // Validate the beat
    if (beat <= 0) {
      message = "Beat in model <" + priceModel + "> and step number <"
              + step + "> is invalid <" + beat + "> in module <"
              + getSymbolicName() + ">";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // See if we already have the cache object for this price
    if (!PriceModelCache.containsKey(priceModel)) {
      // Create the new PriceModel object
      tmpRateCache = new ArrayList<>();
      PriceModelCache.put(priceModel, tmpRateCache);

      // Add it as the first element in the ArrayList
      tmpRMEntry = new RateMapEntry();
      tmpRMEntry.setStep(step);
      tmpRMEntry.setFrom(from);
      tmpRMEntry.setTo(to);
      tmpRMEntry.setBeat(beat);
      tmpRMEntry.setFactor(factor);
      tmpRMEntry.setChargeBase(chargeBase);
      tmpRMEntry.setStartTime(startTime);

      // so add the entry to the new map. No need to order it, it is the first
      tmpRateCache.add(tmpRMEntry);
    } else {
      // Otherwise just add it to the existing rate model
      tmpRateCache = PriceModelCache.get(priceModel);

      // Add the new entry
      tmpRMEntry = new RateMapEntry();
      tmpRMEntry.setStep(step);
      tmpRMEntry.setFrom(from);
      tmpRMEntry.setTo(to);
      tmpRMEntry.setBeat(beat);
      tmpRMEntry.setFactor(factor);
      tmpRMEntry.setChargeBase(chargeBase);
      tmpRMEntry.setStartTime(startTime);

      // Add the object to the ArrayList
      for (i = 0; i < tmpRateCache.size(); i++) {
        // if it is a later step
        if (tmpRateCache.get(i).getStep() > step) {
          // add a null element
          tmpRateCache = insertElementAt(tmpRateCache, tmpRMEntry, i);
          inserted = true;
          break;
        } else if (tmpRateCache.get(i).getStep() == step) {
          // if it is a later time version of the same step
          // see if it goes before or after the current one
          if (tmpRateCache.get(i).getStartTime() > startTime) {
            // inserting
            helperRMEntry = tmpRateCache.get(i);
            tmpRMEntry.setChild(helperRMEntry);
            tmpRateCache.set(i, tmpRMEntry);
          } else if (tmpRateCache.get(i).getStartTime() < startTime) {
            // appending
            tmpRateCache.get(i).setChild(tmpRMEntry);
          } else {
            // cannot have two steps with the same start date
            message = "Two steps in model <" + priceModel + "> and step number <"
                    + step + "> have the same start date <" + startTime + "> in module <"
                    + getSymbolicName() + ">";
            OpenRate.getOpenRateFrameworkLog().error(message);
            throw new InitializationException(message, getSymbolicName());
          }
        }
      }
      if (inserted == false) {
        // we did not insert it, so add it
        tmpRateCache.add(tmpRMEntry);
      }
    }
  }

  /**
   * Get a value from the RateCache. The processing based on the result returned
   * here is evaluated in the twinned processing class, in order to reduce the
   * load on the main framework thread.
   *
   * @param key The price model to recover
   * @return The price model structure containing all of the tiers
   */
  public ArrayList<RateMapEntry> getPriceModel(String key) {

    ArrayList<RateMapEntry> tmpEntry;

    // Get the rate plan
    tmpEntry = PriceModelCache.get(key);

    // and return it
    return tmpEntry;
  }

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

      // Add the object to the vector
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
    double tmpFrom;
    double tmpTo;
    double tmpBeat;
    double tmpFactor;
    double tmpChargeBase;
    int tmpTier;

    int MapsLoaded = 0;
    String PriceModel;
    String tmpGroup;
    String tmpModel;
    String tmpRUM;
    String tmpResource;
    String tmpRUMType;
    String tmpResCtr;
    String tmpStringStartTime = null;
    long tmpStartTime = CommonConfig.LOW_DATE; // default = low date

    // ****** perform the loading of the raw price models ******
    // Find the location of the configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Price Model Data Loading from file for <" + getSymbolicName() + ">");

    // Try to open the file
    try {
      inFile = new BufferedReader(new FileReader(PriceModelDataFile));
    } catch (FileNotFoundException fnfe) {
      message = "Not able to read file : <"
              + PriceModelDataFile + ">. message = <" + fnfe.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // inform the user about the start of the price model phase
    OpenRate.getOpenRateFrameworkLog().info("Starting Price Model Data Loading from file for <" + getSymbolicName() + ">");

    // File open, now get the stuff
    try {
      while (inFile.ready()) {
        tmpFileRecord = inFile.readLine();

        if ((tmpFileRecord.startsWith("#"))
                | tmpFileRecord.trim().equals("")) {
          // Comment line, ignore
        } else {
          RatesLoaded++;
          RateFields = tmpFileRecord.split(";");

          // check we have something we can use - either we expect 7 fields (no
          // date defined) or 9 fields (date defined). Everything else is BAD
          if ((RateFields.length == 7) | (RateFields.length == 8)) {
            // Prepare and add the line
            PriceModel = RateFields[0];
            tmpTier = Integer.valueOf(RateFields[1]);
            tmpFrom = Double.valueOf(RateFields[2]);
            tmpTo = Double.valueOf(RateFields[3]);
            tmpBeat = Double.valueOf(RateFields[4]);
            tmpFactor = Double.valueOf(RateFields[5]);
            tmpChargeBase = Double.valueOf(RateFields[6]);

            // if we have the date, load it, otherwise use the default
            if (RateFields.length == 8) {
              tmpStringStartTime = RateFields[7];
              tmpStartTime = fieldInterpreter.convertInputDateToUTC(tmpStringStartTime);
            }

            if (tmpChargeBase == 0) {
              message = "Error in price model <" + PriceModel
                      + "> in module <" + getSymbolicName()
                      + ">. Charge base cannot be 0.";
              OpenRate.getOpenRateFrameworkLog().fatal(message);
              throw new InitializationException(message, getSymbolicName());
            }

            addPriceModel(PriceModel, tmpTier, tmpFrom, tmpTo, tmpBeat, tmpFactor, tmpChargeBase, tmpStartTime);
          } else {
            // Not a valid number of fields
            message = "Invalid number of fields in price map loading for module <"
                    + getSymbolicName() + "> at line <" + RatesLoaded
                    + ">. Expecting <7> or <8>, but got <" + RateFields.length
                    + ">. Line was <" + tmpFileRecord + ">";
            OpenRate.getOpenRateFrameworkLog().error(message);
            throw new InitializationException(message, getSymbolicName());
          }
        }
      }
    } catch (IOException ex) {
      message = "Error reading input file <" + PriceModelDataFile
              + "> in record <" + RatesLoaded + ">. IO Error.";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    } catch (ArrayIndexOutOfBoundsException ex) {
      message
              = "Error reading input file <" + PriceModelDataFile
              + "> in record <" + RatesLoaded + ">. Malformed Record.";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    } catch (ParseException pe) {
      message
              = "Error converting date from <" + PriceModelDataFile
              + "> in record <" + RatesLoaded + ">. Unexpected date value <" + tmpStringStartTime + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    } finally {
      try {
        inFile.close();
      } catch (IOException ex) {
        message = "Error closing input file <" + PriceModelDataFile
                + ">. message = <" + ex.getMessage() + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message, getSymbolicName());
      }
    }

    OpenRate.getOpenRateFrameworkLog().info(
            "Price Model Data Loading completed. " + RatesLoaded
            + " configuration lines loaded from <" + getSymbolicName()
            + ">");

    // ****** perform the loading of the model descriptors ******
    // Find the location of the configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Price Group Data Loading from file for <" + getSymbolicName() + ">");

    // Try to open the file
    try {
      inFile = new BufferedReader(new FileReader(RUMMapDataFile));
    } catch (FileNotFoundException fnfe) {
      message = "Not able to read file : <"
              + RUMMapDataFile + ">. message = <" + fnfe.getMessage() + ">";
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

          if (RateFields.length == 6) {
            // Prepare and add the line
            tmpGroup = RateFields[0];
            tmpModel = RateFields[1];
            tmpRUM = RateFields[2];
            tmpResource = RateFields[3];
            tmpRUMType = RateFields[4];
            tmpResCtr = RateFields[5];

            addRUMMap(tmpGroup, tmpModel, tmpRUM, tmpResource, tmpRUMType, tmpResCtr);
          } else {
            // Not a valid number of fields
            message = "Invalid number of fields in price map loading for module <"
                    + getSymbolicName() + ">. Expecting <6>, but got <" + RateFields.length
                    + ">. Line was <" + tmpFileRecord + ">";
            OpenRate.getOpenRateFrameworkLog().error(message);
            throw new InitializationException(message, getSymbolicName());
          }
        }
      }
    } catch (IOException ex) {
      message = "Error reading input file <" + RUMMapDataFile
              + "> in record <" + MapsLoaded + ">. IO Error.";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    } catch (ArrayIndexOutOfBoundsException ex) {
      message
              = "Error reading input file <" + RUMMapDataFile
              + "> in record <" + MapsLoaded + ">. Malformed Record.";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    } finally {
      try {
        inFile.close();
      } catch (IOException ex) {
        message = "Error closing input file <" + RUMMapDataFile
                + ">. message = <" + ex.getMessage() + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message, getSymbolicName());
      }
    }

    OpenRate.getOpenRateFrameworkLog().info(
            "Price Group Data Loading completed. " + MapsLoaded
            + " configuration lines loaded from <" + RUMMapDataFile
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
    int mapsLoaded = 0;
    String tmpGroup = null;
    String tmpModel = null;
    String tmpRUM = null;
    String tmpResource = null;
    String tmpRUMType = null;
    String tmpResCtr = null;

    int ratesLoaded = 0;
    int columns;
    String priceModel;
    double tmpFrom;
    double tmpTo;
    double tmpBeat;
    double tmpFactor;
    int tmpTier;
    double tmpChargeBase;
    long tmpStartTime = CommonConfig.LOW_DATE; // default = low date
    String tmpStringStartTime = null;

    // Find the location of the configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting RUM Rate Cache Loading from DB for <" + getSymbolicName() + ">");

    // Try to open the DS
    JDBCcon = DBUtil.getConnection(cacheDataSourceName);

    // ****** perform the loading of the raw price models ******
    // Now prepare the statements
    prepareStatements();

    // inform the user about the start of the price model phase
    OpenRate.getOpenRateFrameworkLog().info("Starting Price Model Data Loading from DB for <" + getSymbolicName() + ">");

    // Execute the query
    try {
      mrs = StmtPriceModelDataSelectQuery.executeQuery();
      columns = mrs.getMetaData().getColumnCount();
    } catch (SQLException ex) {
      message = "Error performing SQL for retieving Price Model Data for <"
              + getSymbolicName() + ">. SQL Error = <" + ex.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // check we have something we can use - either we expect 7 fields (no
    // date defined) or 8 fields (date defined). Everything else is BAD
    if ((columns == 7) | (columns == 8)) {
      // loop through the results for the price model
      try {
        mrs.beforeFirst();

        while (mrs.next()) {
          ratesLoaded++;
          priceModel = mrs.getString(1);
          tmpTier = mrs.getInt(2);
          tmpFrom = mrs.getDouble(3);
          tmpTo = mrs.getDouble(4);
          tmpBeat = mrs.getDouble(5);
          tmpFactor = mrs.getDouble(6);
          tmpChargeBase = mrs.getDouble(7);

          // if we have the date, load it, otherwise use the default
          if (columns == 8) {
            tmpStringStartTime = mrs.getString(8);
            tmpStartTime = fieldInterpreter.convertInputDateToUTC(tmpStringStartTime);
          }

          if (tmpChargeBase == 0) {
            // cannot have a 0 charge base - exception
            message = "Error in price model <" + priceModel
                    + "> in module <" + getSymbolicName()
                    + ">. Charge base cannot be 0.";
            OpenRate.getOpenRateFrameworkLog().fatal(message);
            throw new InitializationException(message, getSymbolicName());
          }

          // Add the map
          addPriceModel(priceModel, tmpTier, tmpFrom, tmpTo, tmpBeat, tmpFactor, tmpChargeBase, tmpStartTime);
        }
      } catch (SQLException ex) {
        message = "Error opening Price Model Data for <" + getSymbolicName()
                + ">. SQL Error = <" + ex.getMessage() + ">";
        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message, getSymbolicName());
      } catch (ParseException pe) {
        message
                = "Error converting date from <" + getSymbolicName() + "> in record <"
                + ratesLoaded + ">. Unexpected date value <" + tmpStringStartTime + ">";
        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message, getSymbolicName());
      }
    } else {
      // Not a valid number of fields
      message = "Invalid number of fields in price map loading for module <"
              + getSymbolicName() + ">. Expecting <7> or <8>, but got <" + columns + ">.";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // Close down stuff
    DBUtil.close(mrs);
    DBUtil.close(StmtPriceModelDataSelectQuery);

    OpenRate.getOpenRateFrameworkLog().info(
            "Price Model Data Loading completed. " + ratesLoaded
            + " configuration lines loaded from <" + getSymbolicName()
            + ">");

    // ****** perform the loading of the model descriptors ******
    // Find the location of the configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Price Group Data Loading from DB for <" + getSymbolicName() + ">");

    // Execute the query
    try {
      mrs = StmtRUMMapDataSelectQuery.executeQuery();
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
      // loop through the results for the price model
      try {
        mrs.beforeFirst();

        while (mrs.next()) {
          mapsLoaded++;
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
    DBUtil.close(mrs);
    DBUtil.close(StmtPriceModelDataSelectQuery);
    DBUtil.close(JDBCcon);

    OpenRate.getOpenRateFrameworkLog().info(
            "Price Group Data Loading completed. " + mapsLoaded
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
    // clear the price model cache
    PriceModelCache.clear();

    // clear the RUM map cache
    RUMMapCache.clear();
  }

  // -----------------------------------------------------------------------------
  // ---------------- Start of data base data layer functions --------------------
  // -----------------------------------------------------------------------------
  /**
   * Get the data files that we are going to be reading, when reading from
   * "File" data source types
   *
   * @return true if the configuration is good, otherwise false.
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  protected boolean getDataFiles(String ResourceName, String CacheName) throws InitializationException {
    // Get the Select statement
    RUMMapDataFile = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
            CacheName,
            "RUMMapDataFile",
            "None");

    // Get the Select statement
    PriceModelDataFile = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
            CacheName,
            "PriceModelDataFile",
            "None");

    if (RUMMapDataFile.equals("None") | PriceModelDataFile.equals("None")) {
      return false;
    } else {
      return true;
    }
  }

  /**
   * get the select statement(s). Implemented as a separate function so that it
   * can be overwritten in implementation classes.
   *
   * @return true if the configuration is good, otherwise false.
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  protected boolean getDataStatements(String ResourceName, String CacheName) throws InitializationException {
    // Get the Select statement
    PriceModelDataSelectQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
            CacheName,
            "PriceModelStatement",
            "None");

    // Get the Select statement
    RUMMapDataSelectQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
            CacheName,
            "RUMMapStatement",
            "None");

    if (PriceModelDataSelectQuery.equals("None") | RUMMapDataSelectQuery.equals("None")) {
      return false;
    } else {
      return true;
    }
  }

  /**
   * PrepareStatements creates the statements from the SQL expressions so that
   * they can be run as needed.
   *
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  protected void prepareStatements()
          throws InitializationException {
    // prepare our statements
    try {
      // prepare the SQL for the TestStatement
      StmtRUMMapDataSelectQuery = JDBCcon.prepareStatement(RUMMapDataSelectQuery,
              ResultSet.TYPE_SCROLL_INSENSITIVE,
              ResultSet.CONCUR_READ_ONLY);
    } catch (SQLException ex) {
      message = "Error preparing the statement " + RUMMapDataSelectQuery;
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    } catch (Exception ex) {
      message = "Error preparing the statement <" + RUMMapDataSelectQuery + ">. message: " + ex.getMessage();
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }

    // prepare our statements
    try {
      // prepare the SQL for the TestStatement
      StmtPriceModelDataSelectQuery = JDBCcon.prepareStatement(PriceModelDataSelectQuery,
              ResultSet.TYPE_SCROLL_INSENSITIVE,
              ResultSet.CONCUR_READ_ONLY);
    } catch (SQLException ex) {
      message = "Error preparing the statement " + PriceModelDataSelectQuery;
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    } catch (Exception ex) {
      message = "Error preparing the statement <" + PriceModelDataSelectQuery + ">. message: " + ex.getMessage();
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }
  }

  /**
   * Simulate insert at (which is not available in ArrayList
   *
   * @param oldList
   * @param audSeg
   * @param i
   * @return
   */
  private ArrayList<RateMapEntry> insertElementAt(ArrayList<RateMapEntry> oldList, RateMapEntry audSeg, int i) {
    ArrayList<RateMapEntry> newList = new ArrayList<>();

    Iterator<RateMapEntry> oldListIter = oldList.iterator();

    int position = 0;
    while (oldListIter.hasNext()) {
      if (position == i) {
        newList.add(audSeg);
      }

      // add the element from the old list
      newList.add(oldListIter.next());
      position++;
    }

    return newList;
  }
}
