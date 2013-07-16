/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2013.
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
 * Tiger Shore Management or its officially assigned agents be liable to any
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
 * Please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=RUM_Rate_Cache'>click here</a> to go to wiki page.
 * <br>
 * <p>
 * This class extends the basic rating scheme found in the "RateCache" module
 * to implement a rating scheme based on full mappable RUM (rateable Usage
 * Metric) model for rating records according to a tier and beat model, as well
 * as impacting multiple resources using multiple RUMs.
 *
 * The data is read in from a configuration file or database, and contains two
 * distinct sorts of information:
 *  - Price Model Map. This describes the price models
 *  - RUM Map. This describes the price models to apply, the RUMs to read and
 *             the resources to impact.
 *
 * For the data source type "File", the data for the Price Model Map will be
 * read from the file you give under "PriceModelDataFile". The RUM Map will be
 * read from the file that you give under "RUMMapDataFile".
 *
 * For the data source type "DB", the data for the Price Model Map will be
 * read from the query you give under "PriceModelStatement". The RUM Map will be
 * read from the query that you give under "RUMMapStatement".
 *
 * In either case, the data that is read is:
 *
 * Price Model Map
 * ---------------
 * PriceModel;Step;TierFrom;TierTo;Beat;Factor;ChargeBase
 *
 * Where:
 *   'PriceModel' is the identifier for a price model
 *   'Step'       is the number of the tier, starting from 1 and incrementing
 *                for each tier to be evaluated
 *   'TierFrom'   is the start of the tier, usually tiers will start from 1
 *   'TierTo'     is the end of the tier
 *   'Beat'       is the granularity of the rating (should be an exact fraction
 *                of the tier)
 *   'Factor'     is the cost of each "charge base" number of units in this tier
 *   'ChargeBase' is the number of units for which the cost factor has been
 *                defined. This lets you define prices as minutes (ChargeBase
 *                = 60), but still rate on a per second basis (Beat = 1).
 *   'StartTime'  is the start of the validity of this step, allowing multiple
 *                validity periods for each step. A value of 0 in this field
 *                means that the step will be valid for ever
 *   'EndTime'    is the end of the validity of this step, allowing multiple
 *                validity periods for each step
 *
 * RUM Map
 * ---------------
 * PriceGroup;PriceModel;RUM;Resource;RUMType;ResCtr
 *
 * Where:
 *   'PriceGroup' is a unique code for the Price Model Group. All price models
 *                within the group will be executed
 *   'PriceModel' is a unique code for the PriceModel
 *   'RUM'        is the name of the RUM to be used by the price model
 *   'Resource'   is the resource to impact
 *   'RUMType'    is the type of rating that should be performed on the RUM, and
 *                can be one of the following:
 *     'Flat'      - will return a simple RUM * Price, without tiers or beats
 *     'Tiered'    - will evaluate all tiers, rating the portion of the RUM that
 *                   lies within the tier individually
 *     'Threshold' - will rate all of the RUM in the tier that the RUM lies in
 *     'Event'     - will return a fixed value regardless of the RUM value
 *   'ResCtr'     is the counter to be impacted for this resource
 *
 * @author i.sparkes
 */
public class RUMRateCache
     extends AbstractSyncLoaderCache
{
 /**
  * RUM Map entry
  */
  public class RUMMapEntry
  {
    /**
     * RUMType indicates the type of rating we are to use
     */
    public int    RUMType;

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
    public int    ResourceCounter;

    /**
     * If this is true, we reduce the amount of RUM to rate after rating
     */
    public boolean ConsumeRUM = false;
  }

 /**
  * This stores all the cacheable data necessary for the definition of the
  * rate plans.
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
  * loadCache is called automatically on startup of the
  * cache factory, as a result of implementing the CacheLoader
  * interface. This should be used to load any data that needs loading, and
  * to set up variables.
  *
  * @param ResourceName The name of the resource to load for
  * @param CacheName The name of the cache to load for
  * @throws InitializationException
  */
  @Override
  public void loadCache(String ResourceName, String CacheName)
                 throws InitializationException
  {
    int initialObjectSize = 1000;

    String tmpInitOjectSize = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                       CacheName,
                                                       "InitialObjectSize",
                                                       "1000");
    try
    {
      initialObjectSize = Integer.parseInt(tmpInitOjectSize);
    }
    catch (NumberFormatException nfe)
    {
      message = "Could not parse initial object size <" + initialObjectSize +
                       "> for cache <" + getSymbolicName() + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // inform the user about the start of the price model phase
    getFWLog().debug("Setting initial hash map size to <" + initialObjectSize + "> for cache <" + getSymbolicName() + ">");

    PriceModelCache = new HashMap<>(initialObjectSize);
    RUMMapCache = new HashMap<>(initialObjectSize);

    // Do the parent initialisation
    super.loadCache(ResourceName,CacheName);
  }

  // -----------------------------------------------------------------------------
  // ----------------------- Start of custom functions ---------------------------
  // -----------------------------------------------------------------------------

 /**
  * Add a value into the RateCache, defining the RateMapEntry result value
  * that should be returned in the case of a match. A PriceModel is therefore
  * defined as a group of RateMapEntries, that make up a whole rate map.
  *
  * @param PriceModel The price model name to add
  * @param Step The tier number (starting from 1) to enable multiple tiers
  * @param From The start of the tier
  * @param To The end of the tier
  * @param Beat The charging granularity
  * @param Factor The value to charge for each beat
  * @param ChargeBase The base amount to charge the factor for
  * @param StartTime The start time of the validity
  * @throws InitializationException
  */
  public void addPriceModel(String PriceModel, int Step, double From, double To,
                            double Beat, double Factor, double ChargeBase,
                            long StartTime)
    throws InitializationException
  {

    ArrayList<RateMapEntry> tmpRateCache;
    RateMapEntry tmpRMEntry;
    RateMapEntry helperRMEntry;
    int i;
    boolean inserted = false;

    // Validate the beat
    if(Beat <= 0)
    {
      message = "Beat in model <" + PriceModel + "> and step number <" +
                        Step + "> is invalid <" + Beat + "> in module <" +
                        getSymbolicName() + ">";
      getFWLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // See if we already have the cache object for this price
    if (!PriceModelCache.containsKey(PriceModel))
    {
      // Create the new PriceModel object
      tmpRateCache = new ArrayList<>();
      PriceModelCache.put(PriceModel, tmpRateCache);

      // Add it as the first element in the ArrayList
      tmpRMEntry = new RateMapEntry();
      tmpRMEntry.setStep(Step);
      tmpRMEntry.setFrom(From);
      tmpRMEntry.setTo(To);
      tmpRMEntry.setBeat(Beat);
      tmpRMEntry.setFactor(Factor);
      tmpRMEntry.setChargeBase(ChargeBase);
      tmpRMEntry.setStartTime(StartTime);
      tmpRMEntry.setEndTime(CommonConfig.HIGH_DATE);

      // so add the entry to the new map. No need to order it, it is the first
      tmpRateCache.add(tmpRMEntry);
    }
    else
    {
      // Otherwise just add it to the existing rate model
      tmpRateCache = PriceModelCache.get(PriceModel);

      // Add the new entry
      tmpRMEntry = new RateMapEntry();
      tmpRMEntry.setStep(Step);
      tmpRMEntry.setFrom(From);
      tmpRMEntry.setTo(To);
      tmpRMEntry.setBeat(Beat);
      tmpRMEntry.setFactor(Factor);
      tmpRMEntry.setChargeBase(ChargeBase);
      tmpRMEntry.setStartTime(StartTime);
      tmpRMEntry.setEndTime(CommonConfig.HIGH_DATE);

      // Add the object to the ArrayList
      for (i = 0 ; i < tmpRateCache.size() ; i++)
      {
        // if it is a later step
        if (tmpRateCache.get(i).getStep() > Step)
        {
          // add a null element
          tmpRateCache = insertElementAt(tmpRateCache,tmpRMEntry,i);
          inserted = true;
          break;
        }

        // if it is a later time version of the same step
        else if (tmpRateCache.get(i).getStep() == Step)
        {
          // see if it goes before or after the current one
          if (tmpRateCache.get(i).getStartTime() > StartTime)
          {
            // inserting
            helperRMEntry = tmpRateCache.get(i);
            tmpRMEntry.setChild(helperRMEntry);
            tmpRMEntry.setEndTime(helperRMEntry.getStartTime() - 1);
            tmpRateCache.set(i, tmpRMEntry);
          }
          else if (tmpRateCache.get(i).getStartTime() < StartTime)
          {
            // appending
            tmpRateCache.get(i).setChild(tmpRMEntry);
            tmpRateCache.get(i).setEndTime(tmpRMEntry.getStartTime() - 1);
          }
          else
          {
            // cannot have two steps with the same start date
            message = "Two steps in model <" + PriceModel + "> and step number <" +
                             Step + "> have the same start date <" + StartTime + "> in module <" +
                             getSymbolicName() + ">";
            getFWLog().error(message);
            throw new InitializationException(message,getSymbolicName());
          }
        }
      }
      if (inserted == false)
      {
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
  public ArrayList<RateMapEntry> getPriceModel(String key)
  {

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
  public void addRUMMap(String PriceGroup, String PriceModel, String RUM, String Resource, String RUMType, String ResourceCounter) throws InitializationException
  {

    ArrayList<RUMMapEntry> tmpRUMMapCache;
    RUMMapEntry tmpRMEntry;

    // See if we already have the cache object for this price
    if (!RUMMapCache.containsKey(PriceGroup))
    {

      // Create the new PriceModel object
      tmpRUMMapCache = new ArrayList<>();
      RUMMapCache.put(PriceGroup, tmpRUMMapCache);
      tmpRMEntry = new RUMMapEntry();
      tmpRMEntry.PriceModel = PriceModel;
      tmpRMEntry.RUM = RUM;
      tmpRMEntry.Resource = Resource;
      tmpRMEntry.ResourceCounter = Integer.parseInt(ResourceCounter);

      if(RUMType.equalsIgnoreCase("flat"))
      {
        tmpRMEntry.RUMType = 1;
      }
      else if(RUMType.equalsIgnoreCase("tiered"))
      {
        tmpRMEntry.RUMType = 2;
      }
      else if(RUMType.equalsIgnoreCase("threshold"))
      {
        tmpRMEntry.RUMType = 3;
      }
      else if(RUMType.equalsIgnoreCase("event"))
      {
        tmpRMEntry.RUMType = 4;
      }
      else
      {
        message = "Unknown rating type <" + RUMType + ">";
        getFWLog().error(message);
        throw new InitializationException(message,getSymbolicName());
      }

      // so add the entry to the new map. No need to order it, it is the first
      tmpRUMMapCache.add(tmpRMEntry);
    }
    else
    {

      // Otherwise just add it to the existing rate model
      tmpRUMMapCache = RUMMapCache.get(PriceGroup);

      // Add the new entry
      tmpRMEntry = new RUMMapEntry();
      tmpRMEntry.PriceModel = PriceModel;
      tmpRMEntry.RUM = RUM;
      tmpRMEntry.Resource = Resource;
      tmpRMEntry.ResourceCounter = Integer.parseInt(ResourceCounter);

      if(RUMType.equalsIgnoreCase("flat"))
      {
        tmpRMEntry.RUMType = 1;
      }
      else if(RUMType.equalsIgnoreCase("tiered"))
      {
        tmpRMEntry.RUMType = 2;
      }
      else if(RUMType.equalsIgnoreCase("threshold"))
      {
        tmpRMEntry.RUMType = 3;
      }
      else if(RUMType.equalsIgnoreCase("event"))
      {
        tmpRMEntry.RUMType = 4;
      }
      else
      {
        message = "Unknown rating type <" + RUMType + ">";
        getFWLog().error(message);
        throw new InitializationException(message,getSymbolicName());
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
  * @return The RUM map containing all of the pricemodel-RUM-Resource combinations
  */
  public ArrayList<RUMMapEntry> getRUMMap(String key)
  {

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
                        throws InitializationException
  {
    // Variable declarations
    int            RatesLoaded = 0;
    BufferedReader inFile;
    String         tmpFileRecord;
    String[]       RateFields;
    double         tmpFrom;
    double         tmpTo;
    double         tmpBeat;
    double         tmpFactor;
    double         tmpChargeBase;
    int            tmpTier;

    int            MapsLoaded = 0;
    String         PriceModel;
    String         tmpGroup;
    String         tmpModel;
    String         tmpRUM;
    String         tmpResource;
    String         tmpRUMType;
    String         tmpResCtr;
    long           tmpStartTime = CommonConfig.LOW_DATE; // default = low date
    String         tmpStringStartTime = null;

    // ****** perform the loading of the raw price models ******

    // Find the location of the configuration file
    getFWLog().info("Starting Price Model Data Loading from file for <" + getSymbolicName() + ">");

    // Try to open the file
    try
    {
      inFile = new BufferedReader(new FileReader(PriceModelDataFile));
    }
    catch (FileNotFoundException fnfe)
    {
      message = "Not able to read file : <" +
            PriceModelDataFile + ">. message = <" + fnfe.getMessage() + ">";
      getFWLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // inform the user about the start of the price model phase
    getFWLog().info("Starting Price Model Data Loading from file for <" + getSymbolicName() + ">");

    // File open, now get the stuff
    try
    {
      while (inFile.ready())
      {
        tmpFileRecord = inFile.readLine();

        if ((tmpFileRecord.startsWith("#")) |
            tmpFileRecord.trim().equals(""))
        {
          // Comment line, ignore
        }
        else
        {
          RatesLoaded++;
          RateFields = tmpFileRecord.split(";");

          // check we have something we can use - either we expect 7 fields (no
          // date defined) or 9 fields (date defined). Everything else is BAD
          if ((RateFields.length == 7) | (RateFields.length == 8))
          {
            // Prepare and add the line
            PriceModel = RateFields[0];
            tmpTier = Integer.valueOf(RateFields[1]);
            tmpFrom = Double.valueOf(RateFields[2]);
            tmpTo = Double.valueOf(RateFields[3]);
            tmpBeat = Double.valueOf(RateFields[4]);
            tmpFactor = Double.valueOf(RateFields[5]);
            tmpChargeBase = Double.valueOf(RateFields[6]);

            // if we have the date, load it, otherwise use the default
            if (RateFields.length == 8)
            {
              tmpStringStartTime = RateFields[7];
              tmpStartTime = fieldInterpreter.convertInputDateToUTC(tmpStringStartTime);
            }

            if (tmpChargeBase == 0)
            {
              message = "Error in price model <" + PriceModel +
                               "> in module <" + getSymbolicName() +
                               ">. Charge base cannot be 0.";
              getFWLog().fatal(message);
              throw new InitializationException(message,getSymbolicName());
            }

            addPriceModel(PriceModel, tmpTier, tmpFrom, tmpTo, tmpBeat, tmpFactor, tmpChargeBase, tmpStartTime);
          }
          else
          {
            // Not a valid number of fields
            message = "Invalid number of fields in price map loading for module <" +
                             getSymbolicName() + "> at line <" + RatesLoaded +
                             ">. Expecting <7> or <8>, but got <" + RateFields.length +
                             ">. Line was <" + tmpFileRecord + ">";
            getFWLog().error(message);
            throw new InitializationException(message,getSymbolicName());
          }
        }
      }
    }
    catch (IOException ex)
    {
      message = "Error reading input file <" + PriceModelDataFile +
            "> in record <" + RatesLoaded + ">. IO Error.";
      getFWLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }
    catch (ArrayIndexOutOfBoundsException ex)
    {
      message =
            "Error reading input file <" + PriceModelDataFile +
            "> in record <" + RatesLoaded + ">. Malformed Record.";
      getFWLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }
    catch (ParseException pe)
    {
      message =
            "Error converting date from <" + PriceModelDataFile +
            "> in record <" + RatesLoaded + ">. Unexpected date value <" + tmpStringStartTime + ">";
      getFWLog().fatal(message);
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
        message = "Error closing input file <" + PriceModelDataFile +
                  ">. message = <" + ex.getMessage() + ">";
        getFWLog().error(message);
        throw new InitializationException(message,getSymbolicName());
      }
    }

    getFWLog().info(
          "Price Model Data Loading completed. " + RatesLoaded +
          " configuration lines loaded from <" + getSymbolicName() +
          ">");

    // ****** perform the loading of the model descriptors ******
    // Find the location of the configuration file
    getFWLog().info("Starting Price Group Data Loading from file for <" + getSymbolicName() + ">");

    // Try to open the file
    try
    {
      inFile = new BufferedReader(new FileReader(RUMMapDataFile));
    }
    catch (FileNotFoundException fnfe)
    {
      message = "Not able to read file : <" +
            RUMMapDataFile + ">. message = <" + fnfe.getMessage() + ">";
      getFWLog().error(message);
      throw new InitializationException(message,getSymbolicName());
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
          // Comment line, ignore
        }
        else
        {
          MapsLoaded++;
          RateFields = tmpFileRecord.split(";");

          if (RateFields.length == 6)
          {
            // Prepare and add the line
            tmpGroup    = RateFields[0];
            tmpModel    = RateFields[1];
            tmpRUM      = RateFields[2];
            tmpResource = RateFields[3];
            tmpRUMType  = RateFields[4];
            tmpResCtr   = RateFields[5];

            addRUMMap(tmpGroup, tmpModel, tmpRUM, tmpResource, tmpRUMType, tmpResCtr);
          }
          else
          {
            // Not a valid number of fields
            message = "Invalid number of fields in price map loading for module <" +
                             getSymbolicName() + ">. Expecting <6>, but got <" + RateFields.length +
                             ">. Line was <" + tmpFileRecord + ">";
            getFWLog().error(message);
            throw new InitializationException(message,getSymbolicName());
          }
        }
      }
    }
    catch (IOException ex)
    {
      message = "Error reading input file <" + RUMMapDataFile +
            "> in record <" + MapsLoaded + ">. IO Error.";
      getFWLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }
    catch (ArrayIndexOutOfBoundsException ex)
    {
      message =
            "Error reading input file <" + RUMMapDataFile +
            "> in record <" + MapsLoaded + ">. Malformed Record.";
      getFWLog().fatal(message);
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
        message = "Error closing input file <" + RUMMapDataFile +
                  ">. message = <" + ex.getMessage() + ">";
        getFWLog().error(message);
        throw new InitializationException(message,getSymbolicName());
      }
    }

    getFWLog().info(
          "Price Group Data Loading completed. " + MapsLoaded +
          " configuration lines loaded from <" + RUMMapDataFile +
          ">");
  }

 /**
  * Load the data from the defined Data Source
  *
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void loadDataFromDB()
                      throws InitializationException
  {
    int            MapsLoaded = 0;
    String         tmpGroup = null;
    String         tmpModel = null;
    String         tmpRUM = null;
    String         tmpResource = null;
    String         tmpRUMType = null;
    String         tmpResCtr = null;

    int            RatesLoaded = 0;
    int            Columns;
    String         PriceModel;
    double         tmpFrom;
    double         tmpTo;
    double         tmpBeat;
    double         tmpFactor;
    int            tmpTier;
    double         tmpChargeBase;
    long           tmpStartTime = CommonConfig.LOW_DATE; // default = low date
    String         tmpStringStartTime = null;

    // Find the location of the configuration file
    getFWLog().info("Starting RUM Rate Cache Loading from DB for <" + getSymbolicName() + ">");

    // Try to open the DS
    JDBCcon = DBUtil.getConnection(cacheDataSourceName);

    // ****** perform the loading of the raw price models ******
    // Now prepare the statements
    prepareStatements();

    // inform the user about the start of the price model phase
    getFWLog().info("Starting Price Model Data Loading from DB for <" + getSymbolicName() + ">");

    // Execute the query
    try
    {
      mrs = StmtPriceModelDataSelectQuery.executeQuery();
      Columns = mrs.getMetaData().getColumnCount();
    }
    catch (SQLException ex)
    {
      message = "Error performing SQL for retieving Price Model Data for <" +
                       getSymbolicName() + ">. SQL Error = <" + ex.getMessage() + ">";
      getFWLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // check we have something we can use - either we expect 7 fields (no
    // date defined) or 8 fields (date defined). Everything else is BAD
    if ((Columns == 7) | (Columns == 8))
    {
      // loop through the results for the price model
      try
      {
        mrs.beforeFirst();

        while (mrs.next())
        {
          RatesLoaded++;
          PriceModel    = mrs.getString(1);
          tmpTier       = mrs.getInt(2);
          tmpFrom       = mrs.getDouble(3);
          tmpTo         = mrs.getDouble(4);
          tmpBeat       = mrs.getDouble(5);
          tmpFactor     = mrs.getDouble(6);
          tmpChargeBase = mrs.getDouble(7);

          // if we have the date, load it, otherwise use the default
          if (Columns == 8)
          {
            tmpStringStartTime = mrs.getString(8);
            tmpStartTime = fieldInterpreter.convertInputDateToUTC(tmpStringStartTime);
          }

          if (tmpChargeBase == 0)
          {
            // cannot have a 0 charge base - exception
            message = "Error in price model <" + PriceModel +
                             "> in module <" + getSymbolicName() +
                             ">. Charge base cannot be 0.";
            getFWLog().fatal(message);
            throw new InitializationException(message,getSymbolicName());
          }

          // Add the map
          addPriceModel(PriceModel, tmpTier, tmpFrom, tmpTo, tmpBeat, tmpFactor, tmpChargeBase, tmpStartTime);
        }
      }
      catch (SQLException ex)
      {
        message = "Error opening Price Model Data for <" + getSymbolicName() +
              ">. SQL Error = <" + ex.getMessage() + ">";
        getFWLog().fatal(message);
        throw new InitializationException(message,getSymbolicName());
      }
      catch (ParseException pe)
      {
        message =
              "Error converting date from <" + getSymbolicName() + "> in record <" +
              RatesLoaded + ">. Unexpected date value <" + tmpStringStartTime + ">";
        getFWLog().fatal(message);
        throw new InitializationException(message,getSymbolicName());
      }
    }
    else
    {
      // Not a valid number of fields
      message = "Invalid number of fields in price map loading for module <" +
                       getSymbolicName() + ">. Expecting <7> or <8>, but got <" + Columns + ">.";
      getFWLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // Close down stuff
    try
    {
      mrs.close();
      StmtPriceModelDataSelectQuery.close();
    }
    catch (SQLException ex)
    {
      message = "Error closing Price Model Data connection for <" +
                       getSymbolicName() + ">. SQL Error = <" + ex.getMessage() + ">";
      getFWLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    getFWLog().info(
          "Price Model Data Loading completed. " + RatesLoaded +
          " configuration lines loaded from <" + getSymbolicName() +
          ">");

    // ****** perform the loading of the model descriptors ******
    // Find the location of the configuration file
    getFWLog().info("Starting Price Group Data Loading from DB for <" + getSymbolicName() + ">");

    // Execute the query
    try
    {
      mrs = StmtRUMMapDataSelectQuery.executeQuery();
    }
    catch (SQLException ex)
    {
      message = "Error performing SQL for retieving Price Group Data for <" +
                       getSymbolicName() + ">. SQL Error = <" + ex.getMessage() + ">";
      getFWLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // check we have something we can use - we expect 7 fields. Everything
    // else is BAD
    if (Columns == 7)
    {
      // loop through the results for the price model
      try
      {
        mrs.beforeFirst();

        while (mrs.next())
        {
          MapsLoaded++;
        tmpGroup    = mrs.getString(1);
        tmpModel    = mrs.getString(2);
        tmpRUM      = mrs.getString(3);
        tmpResource = mrs.getString(4);
        tmpRUMType  = mrs.getString(5);
        tmpResCtr   = mrs.getString(6);

          // Add the map
        addRUMMap(tmpGroup, tmpModel, tmpRUM, tmpResource, tmpRUMType, tmpResCtr);
        }
      }
      catch (SQLException Sex)
      {
        message = "Error opening Price Group Data for <" +
                         getSymbolicName() + ">. SQL Error = <" + Sex.getMessage() + ">";
        getFWLog().fatal(message);
        throw new InitializationException(message,getSymbolicName());
      }
      catch (NullPointerException npe)
      {
        message = "Null value loading Price Group Data for <" +
                         getSymbolicName() + ">. Group <" + tmpGroup +
                         ">, Model <" + tmpModel + ">, RUM <" + tmpRUM +
                         ">, Resource <" + tmpResource + ">, RUM Type <" +
                         tmpRUMType + ">, Step <" + tmpResCtr + ">";
        getFWLog().fatal(message);
        throw new InitializationException(message,getSymbolicName());
      }
    }
    else
    {
      // Not a valid number of fields
      message = "Invalid number of fields in rum map loading for module <" +
                       getSymbolicName() + ">. Expecting <7>, but got <" + Columns + ">.";
      getFWLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // Close down stuff
    try
    {
      mrs.close();
      StmtRUMMapDataSelectQuery.close();
      JDBCcon.close();
    }
    catch (SQLException ex)
    {
      message = "Error closing Price Group Data connection for <" +
            getSymbolicName() + ">. SQL Error = <" + ex.getMessage() + ">";
      getFWLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    getFWLog().info(
          "Price Group Data Loading completed. " + MapsLoaded +
          " configuration lines loaded from <" + getSymbolicName() +
          ">");
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

 /**
  * Clear down the cache contents in the case that we are ordered to reload
  */
  @Override
  public void clearCacheObjects()
  {
    // clear the price model cache
    PriceModelCache.clear();

    // clear the RUM map cache
    RUMMapCache.clear();
  }

  // -----------------------------------------------------------------------------
  // ---------------- Start of data base data layer functions --------------------
  // -----------------------------------------------------------------------------

 /**
  * Get the data files that we are going to be reading, when reading from "File"
  * data source types
  *
  * @return true if the configuration is good, otherwise false.
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  protected boolean getDataFiles(String ResourceName, String CacheName) throws InitializationException
  {
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

    if (RUMMapDataFile.equals("None") | PriceModelDataFile.equals("None"))
    {
      return false;
    }
    else
    {
      return true;
    }
  }

 /**
  * get the select statement(s). Implemented as a separate function so that it can
  * be overwritten in implementation classes.
  *
  * @return true if the configuration is good, otherwise false.
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  protected boolean getDataStatements(String ResourceName, String CacheName) throws InitializationException
  {
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

    if (PriceModelDataSelectQuery.equals("None") | RUMMapDataSelectQuery.equals("None"))
    {
      return false;
    }
    else
    {
      return true;
    }
  }

  /**
  * PrepareStatements creates the statements from the SQL expressions
  * so that they can be run as needed.
   *
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  protected void prepareStatements()
                          throws InitializationException
  {
    // prepare our statements
    try
    {
      // prepare the SQL for the TestStatement
      StmtRUMMapDataSelectQuery = JDBCcon.prepareStatement(RUMMapDataSelectQuery,
                                                      ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                      ResultSet.CONCUR_READ_ONLY);
    }
    catch (SQLException ex)
    {
      message = "Error preparing the statement " + RUMMapDataSelectQuery;
      getFWLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }
    catch (Exception ex)
    {
      message = "Error preparing the statement <" + RUMMapDataSelectQuery + ">. message: " + ex.getMessage();
      getFWLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // prepare our statements
    try
    {
      // prepare the SQL for the TestStatement
      StmtPriceModelDataSelectQuery = JDBCcon.prepareStatement(PriceModelDataSelectQuery,
                                                      ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                      ResultSet.CONCUR_READ_ONLY);
    }
    catch (SQLException ex)
    {
      message = "Error preparing the statement " + PriceModelDataSelectQuery;
      getFWLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }
    catch (Exception ex)
    {
      message = "Error preparing the statement <" + PriceModelDataSelectQuery + ">. message: " + ex.getMessage();
      getFWLog().error(message);
      throw new InitializationException(message,getSymbolicName());
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
  private ArrayList<RateMapEntry> insertElementAt(ArrayList<RateMapEntry> oldList, RateMapEntry audSeg, int i)
  {
    ArrayList<RateMapEntry> newList = new ArrayList<>();

    Iterator<RateMapEntry> oldListIter = oldList.iterator();

    int position = 0;
    while (oldListIter.hasNext())
    {
      if (position == i)
      {
        newList.add(audSeg);
      }

      // add the element from the old list
      newList.add(oldListIter.next());
      position++;
    }

    return newList;
  }
}
