
package OpenRate.cache;

import OpenRate.CommonConfig;
import OpenRate.OpenRate;
import static OpenRate.cache.RUMRateCache.PriceModelDataFile;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.record.RateMapEntry;
import OpenRate.utils.PropertyUtils;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Please
 * <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Rate_Cache'>click
 * here</a> to go to wiki page.
 * <br>
 * <p>
 * This class implements a basic rating scheme based on a single RUM (rateable
 * Usage Metric) model for rating records according to a tier and beat model.
 *
 * To choose a rating model to be used in the rating, the "zone result" is used.
 * The record is then rated according to a numerical field in the record.
 *
 * Rating uses a tiered model, and within each tier, the beat and the rating
 * factor are considered.
 *
 * The data is read in from a configuration file in this example, and the format
 * of the data to be read in is:
 *
 * PriceModel;Tier;TierFrom;TierTo;Beat;Charge;ChargeBase
 *
 * Where: 'PriceModel' is the identifier for a price model 'Tier' is the number
 * of the tier, starting from 1 and incrementing for each tier to be evaluated
 * 'TierFrom' is the start of the tier, usually tiers will start from 1 'TierTo'
 * is the end of the tier 'Beat' is the granularity of the rating (should be an
 * exact fraction of the tier) 'Charge' is the cost of each "charge base" number
 * of usits in this tier 'ChargeBase' is the number of units for which the cost
 * factor has been defined. This lets you define prices as minutes (ChargeBase =
 * 60), but still rate on a per second basis (Beat = 1).
 *
 * @author i.sparkes
 */
public class RateCache
        extends AbstractSyncLoaderCache {

  /**
   * This stores all the cacheable data necessary for the definition of the rate
   * plans.
   */
  protected HashMap<String, ArrayList<RateMapEntry>> PriceModelCache;

  // -----------------------------------------------------------------------------
  // ----------------------- Start of custom functions ---------------------------
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

    // Do the parent initialisation
    super.loadCache(ResourceName, CacheName);
  }

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
          // if it is a different time version of the same step
          // see if it goes before or after the current one
          if (tmpRateCache.get(i).getStartTime() < startTime) {
            helperRMEntry = tmpRateCache.get(i);
            tmpRMEntry.setChild(helperRMEntry);
            tmpRateCache.set(i, tmpRMEntry);
            inserted = true;
            break;
          } else if (tmpRateCache.get(i).getStartTime() > startTime) {
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
   * @param PriceModel The price model to get
   * @return The ArrayList of price model tiers
   */
  public ArrayList<RateMapEntry> getPriceModel(String PriceModel) {

    ArrayList<RateMapEntry> tmpEntry;

    // Get the rate plan
    tmpEntry = PriceModelCache.get(PriceModel);

    // and return it
    return tmpEntry;
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
    int RatesLoaded = 0;
    BufferedReader inFile;
    String tmpFileRecord;
    String[] RateFields;
    double tmpFrom;
    double tmpTo;
    double tmpBeat;
    double tmpFactor;
    int tmpTier;
    double tmpChargeBase;
    String PriceModel;
    String tmpStringStartTime = null;
    long tmpStartTime = CommonConfig.LOW_DATE; // default = low date

    // Find the location of the configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Rate Cache Loading from File");

    // Try to open the file
    try {
      inFile = new BufferedReader(new FileReader(cacheDataFile));
    } catch (FileNotFoundException ex) {

      message = "Application is not able to read file : <" + cacheDataFile + ">";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message, ex, getSymbolicName());
    }

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
            // bad record, log but try to continue
            message = "Error reading input file <" + cacheDataFile
                    + "> in record <" + RatesLoaded + ">. Malformed Record <" + tmpFileRecord
                    + ">. Expecting <7> fields but got <" + RateFields.length + ">";
            OpenRate.getOpenRateFrameworkLog().error(message);
          } else {
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

            // Add the entry to the cache
            addPriceModel(PriceModel, tmpTier, tmpFrom, tmpTo, tmpBeat, tmpFactor, tmpChargeBase, tmpStartTime);

            // Update to the log file
            if ((RatesLoaded % loadingLogNotificationStep) == 0) {
              message = "Rate Cache Data Loading: <" + RatesLoaded
                      + "> configurations loaded for <" + getSymbolicName() + "> from <"
                      + cacheDataFile + ">";
              OpenRate.getOpenRateFrameworkLog().info(message);
            }
          }
        }
      }
    } catch (IOException ex) {
      OpenRate.getOpenRateFrameworkLog().fatal(
              "Error reading input file <" + cacheDataFile
              + "> in record <" + RatesLoaded + ">. IO Error.");
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
        OpenRate.getOpenRateFrameworkLog().error("Error closing input file <" + cacheDataFile
                + ">", ex);
      }
    }

    OpenRate.getOpenRateFrameworkLog().info(
            "Rate Cache Data Loading completed. " + RatesLoaded
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
    int RatesLoaded = 0;
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
    OpenRate.getOpenRateFrameworkLog().info("Starting Rate Cache Loading from DB");

    // Try to open the DS
    JDBCcon = DBUtil.getConnection(cacheDataSourceName);

    // Now prepare the statements
    prepareStatements();

    // Execute the query
    try {
      mrs = StmtCacheDataSelectQuery.executeQuery();
      columns = mrs.getMetaData().getColumnCount();
    } catch (SQLException ex) {
      message = "Error performing SQL for retieving Rate Cache data";
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
          RatesLoaded++;
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

          // Add the map
          addPriceModel(priceModel, tmpTier, tmpFrom, tmpTo, tmpBeat, tmpFactor, tmpChargeBase, tmpStartTime);

          // Update to the log file
          if ((RatesLoaded % loadingLogNotificationStep) == 0) {
            message = "Rate Cache Data Loading: <" + RatesLoaded
                    + "> configurations loaded for <" + getSymbolicName() + "> from <"
                    + cacheDataSourceName + ">";
            OpenRate.getOpenRateFrameworkLog().info(message);
          }
        }
      } catch (SQLException ex) {
        message = "Error opening Search Map Data for <" + cacheDataSourceName + ">";
        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message, ex, getSymbolicName());
      } catch (ParseException pe) {
        message
                = "Error converting date from <" + getSymbolicName() + "> in record <"
                + RatesLoaded + ">. Unexpected date value <" + tmpStringStartTime + ">";
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
    DBUtil.close(StmtCacheDataSelectQuery);
    DBUtil.close(JDBCcon);

    OpenRate.getOpenRateFrameworkLog().info(
            "Rate Cache Data Loading completed. " + RatesLoaded
            + " configuration lines loaded from <" + cacheDataSourceName
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
    PriceModelCache.clear();
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
