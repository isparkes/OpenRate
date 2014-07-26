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
 * This class extends the basic rating scheme found in the "RateCache" module
 * to implement a rating scheme based on what is found in the charge packets
 * only. This means that a rating based on information that you are able to
 * fully define can be performed.
 *
 * This module is usually used with the "RUMMapCache" module, which expands a
 * price group into RUM/Resource pairs, which are then rateable directly. Thus
 * the RUMCPRateCache/RUMMapCache pair more or less equals the RUMRateCache.
 *
 * The data is read in from a configuration file or database:
 *  - Price Model Map. This describes the price models
 *
 * For the data source type "File", the data for the Price Model Map will be
 * read from the file you give under "PriceModelDataFile".
 *
 * For the data source type "DB", the data for the Price Model Map will be
 * read from the query you give under "PriceModelStatement".
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
 *   'Factor'     is the cost of each "charge base" number of usits in this tier
 *   'ChargeBase' is the number of units for which the cost factor has been
 *                defined. This lets you define prices as minutes (ChargeBase
 *                = 60), but still rate on a per second basis (Beat = 1).
 *   'StartTime'  is the start of the validity of this step, allowing multiple
 *                validity periods for each step. A value of 0 in this field
 *                means that the step will be valid for ever
 *   'EndTime'    is the end of the validity of this step, allowing multiple
 *                validity periods for each step
 *
 * @author i.sparkes
 */
public class RUMCPRateCache
     extends AbstractSyncLoaderCache
{
 /**
  * This stores all the cacheable data necessary for the definition of the
  * rate plans.
  */
  protected HashMap<String, ArrayList<RateMapEntry>> PriceModelCache;

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
   * this is the name of the file that holds price models
   */
  protected static String PriceModelDataFile;

 /** Constructor
  * Creates a new instance of the Plan Cache. The plan cache
  * contains all of the Rate Maps that are later cached. The lookup
  * is therefore performed for the defined rate map, passing this back to the
  * rating processing module
  */
  public RUMCPRateCache()
  {
    super();

    PriceModelCache = new HashMap<>(50);
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
  * @param ChargeBase The charge base for the price model
  * @param StartTime The start time to add the time from
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
      OpenRate.getOpenRateFrameworkLog().error(message);
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

      // Add the object to the vector
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
            OpenRate.getOpenRateFrameworkLog().error(message);
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
    PriceModelDataFile = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                     CacheName,
                                                                     "PriceModelDataFile",
                                                                     "None");

    if (PriceModelDataFile.equals("None"))
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

    if (PriceModelDataSelectQuery.equals("None"))
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
      StmtPriceModelDataSelectQuery = JDBCcon.prepareStatement(PriceModelDataSelectQuery,
                                                      ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                      ResultSet.CONCUR_READ_ONLY);
    }
    catch (SQLException ex)
    {
      message = "Error preparing the statement " + PriceModelDataSelectQuery;
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }
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
    String         PriceModel;
    long           tmpStartTime = CommonConfig.LOW_DATE; // default = low date
    String         tmpStringStartTime = null;

    // ****** perform the loading of the raw price models ******

    // Find the location of the configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting RUM Rate Cache Loading from file for <" + getSymbolicName() + ">");

    // Try to open the file
    try
    {
      inFile = new BufferedReader(new FileReader(cacheDataFile));
    }
    catch (FileNotFoundException fnfe)
    {
      message = "Not able to read file : <" +
            cacheDataFile + ">. message = <" + fnfe.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // inform the user about the start of the price model phase
    OpenRate.getOpenRateFrameworkLog().info("Starting Price Model Data Loading from file for <" + getSymbolicName() + ">");

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
              OpenRate.getOpenRateFrameworkLog().fatal(message);
              throw new InitializationException(message,getSymbolicName());
            }

            addPriceModel(PriceModel, tmpTier, tmpFrom, tmpTo, tmpBeat, tmpFactor, tmpChargeBase, tmpStartTime);
          }
          else
          {
            // Not a valid number of fields
            message = "Invalid number of fields in price map loading for module <" +
                             getSymbolicName() + "> at line <" + RatesLoaded +
                             ">. Expecting <7> or <8>, but got <" + RateFields.length + ">.";
            OpenRate.getOpenRateFrameworkLog().error(message);
            throw new InitializationException(message,getSymbolicName());
          }
        }
      }
    }
    catch (IOException ex)
    {
      message = "Error reading input file <" + cacheDataFile +
            "> in record <" + RatesLoaded + ">. IO Error.";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }
    catch (ArrayIndexOutOfBoundsException ex)
    {
      message =
            "Error reading input file <" + cacheDataFile +
            "> in record <" + RatesLoaded + ">. Malformed Record.";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }
    catch (ParseException pe)
    {
      message =
            "Error converting date from <" + cacheDataFile +
            "> in record <" + RatesLoaded + ">. Unexpected date value <" + tmpStringStartTime + ">";
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
        message = "Error closing input file <" + cacheDataFile +
                  ">. message = <" + ex.getMessage() + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message,getSymbolicName());
      }
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Price Model Data Loading completed. " + RatesLoaded +
          " configuration lines loaded from <" + cacheDataFile +
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
    OpenRate.getOpenRateFrameworkLog().info("Starting RUM Rate Cache Loading from DB for <" + getSymbolicName() + ">");

    // Try to open the DS
    JDBCcon = DBUtil.getConnection(cacheDataSourceName);

    // ****** perform the loading of the raw price models ******
    // Now prepare the statements
    prepareStatements();

    // inform the user about the start of the price model phase
    OpenRate.getOpenRateFrameworkLog().info("Starting Price Model Data Loading from DB for <" + getSymbolicName() + ">");

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
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // check we have something we can use - either we expect 7 fields (no
    // date defined) or 9 fields (date defined). Everything else is BAD
    if ((Columns == 7) | (Columns == 8))
    {
      // loop through the results for the customer login cache
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
            OpenRate.getOpenRateFrameworkLog().fatal(message);
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
        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message,getSymbolicName());
      }
      catch (ParseException pe)
      {
        message =
              "Error converting date from <" + getSymbolicName() + "> in record <" +
              RatesLoaded + ">. Unexpected date value <" + tmpStringStartTime + ">";
        OpenRate.getOpenRateFrameworkLog().fatal(message);
        throw new InitializationException(message,getSymbolicName());
      }
    }
    else
    {
      // Not a valid number of fields
      message = "Invalid number of fields in price map loading for module <" +
                       getSymbolicName() + ">. Expecting <7> or <9>, but got <" + Columns + ">.";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // Close down stuff
    try
    {
      mrs.close();
      StmtPriceModelDataSelectQuery.close();
      JDBCcon.close();
    }
    catch (SQLException ex)
    {
      message = "Error closing Price Model Data connection for <" +
                       getSymbolicName() + ">. SQL Error = <" + ex.getMessage() + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Price Model Data Loading completed. " + RatesLoaded +
          " configuration lines loaded from <" + getSymbolicName() +
          ">");
  }

 /**
  * Load the data from the defined Data Source Method
  * 
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void loadDataFromMethod() throws InitializationException
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
  }

 /**
  * Simulate "insert at" (which is not available in ArrayList
  *
  * @param oldList
  * @param newElement
  * @param i
  * @return
  */
  private ArrayList<RateMapEntry> insertElementAt(ArrayList<RateMapEntry> oldList, RateMapEntry newElement, int i)
  {
    ArrayList<RateMapEntry> newList = new ArrayList<>();

    Iterator<RateMapEntry> oldListIter = oldList.iterator();

    int position = 0;
    while (oldListIter.hasNext())
    {
      // if we're in the right place, add the element
      if (position == i)
      {
        newList.add(newElement);
      }

      // add the element from the old list
      newList.add(oldListIter.next());
      position++;
    }

    return newList;
  }
}

