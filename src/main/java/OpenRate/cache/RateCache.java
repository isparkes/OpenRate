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
import OpenRate.OpenRate;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.record.RateMapEntry;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Rate_Cache'>click here</a> to go to wiki page.
 * <br>
 * <p>
 * This class implements a basic rating scheme based on a single RUM (rateable
 * Usage Metric) model for rating records according to a tier and beat model.
 *
 * To choose a rating model to be used in the rating, the "zone result" is
 * used. The record is then rated according to a numerical field in the
 * record.
 *
 * Rating uses a tiered model, and within each tier, the beat and the rating
 * factor are considered.
 *
 * The data is read in from a configuration file in this example, and the
 * format of the data to be read in is:
 *
 * PriceModel;Tier;TierFrom;TierTo;Beat;Charge;ChargeBase
 *
 * Where:
 *   'PriceModel' is the identifier for a price model
 *   'Tier'       is the number of the tier, starting from 1 and incrementing
 *                for each tier to be evaluated
 *   'TierFrom'   is the start of the tier, usually tiers will start from 1
 *   'TierTo'     is the end of the tier
 *   'Beat'       is the granularity of the rating (should be an exact fraction
 *                of the tier)
 *   'Charge'     is the cost of each "charge base" number of usits in this tier
 *   'ChargeBase' is the number of units for which the cost factor has been
 *                defined. This lets you define prices as minutes (ChargeBase
 *                = 60), but still rate on a per second basis (Beat = 1).
 *
 * @author i.sparkes
 */
public class RateCache
     extends AbstractSyncLoaderCache
{
 /**
  * This stores all the cacheable data necessary for the definition of the
  * rate plans.
  */
  protected HashMap<String, ArrayList<RateMapEntry>> PriceModelCache;

 /** Constructor
  * Creates a new instance of the Plan Cache. The plan cache
  * contains all of the Rate Maps that are later cached. The lookup
  * is therefore performed for the defined rate map, passing this back to the
  * rating processing module
  */
  public RateCache()
  {
    super();

    PriceModelCache = new HashMap<>(50);
  }

 /**
  * Add a value into the RateCache, defining the RateMapEntry result value
  * that should be returned in the case of a match. A PriceModel is therefore
  * defined as a group of RateMapEntries, that make up a whole rate map.
  *
  * @param PriceModel The price model to add
  * @param Step The tier (step) to add
  * @param From The value at which the tier starts
  * @param To The value at which the tier ends
  * @param Beat The granularity of the charging
  * @param Charge The factor (price)
  * @param ChargeBase The number of units for which the charge applies
  * @throws InitializationException
  */
  public void addPriceModel(String PriceModel, int Step, double From, double To,
                       double Beat, double Charge, double ChargeBase)
    throws InitializationException
  {
    ArrayList<RateMapEntry> tmpRateCache;
    RateMapEntry tmpRMEntry;
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
      tmpRMEntry = new RateMapEntry();
      tmpRMEntry.setStep(Step);
      tmpRMEntry.setFrom(From);
      tmpRMEntry.setTo(To);
      tmpRMEntry.setBeat(Beat);
      tmpRMEntry.setFactor(Charge);
      tmpRMEntry.setChargeBase(ChargeBase);
      tmpRMEntry.setStartTime(0);
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
      tmpRMEntry.setFactor(Charge);
      tmpRMEntry.setChargeBase(ChargeBase);
      tmpRMEntry.setStartTime(0);
      tmpRMEntry.setEndTime(CommonConfig.HIGH_DATE);

      // Add the object to the ArrayList, creating empty elements if required
      for (i = 0 ; i < tmpRateCache.size() ; i++)
      {
        if (tmpRateCache.get(i).getStep() > Step)
        {
          // add a null element
          tmpRateCache = insertElementAt(tmpRateCache,tmpRMEntry,i);
          inserted = true;
          break;
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
  * @param PriceModel The price model to get
  * @return The ArrayList of price model tiers
  */
  public ArrayList<RateMapEntry> getPriceModel(String PriceModel)
  {

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
    int            tmpTier;
    double         tmpChargeBase;
    String         PriceModel;

    // Find the location of the configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Rate Cache Loading from File");

    // Try to open the file
    try
    {
      inFile = new BufferedReader(new FileReader(CacheDataFile));
    }
    catch (FileNotFoundException ex)
    {
        
      message = "Application is not able to read file : <" + CacheDataFile + ">";
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
          // Comment line, ignore
        }
        else
        {
          RatesLoaded++;
          RateFields = tmpFileRecord.split(";");

          if (RateFields.length != 7)
          {
            // bad record, log but try to continue
            message = "Error reading input file <" + CacheDataFile +
            "> in record <" + RatesLoaded + ">. Malformed Record <" + tmpFileRecord +
            ">. Expecting <7> fields but got <" + RateFields.length + ">";
            OpenRate.getOpenRateFrameworkLog().error(message);
          }
          else
          {
            // Prepare and add the line
            PriceModel = RateFields[0];
            tmpTier = Integer.valueOf(RateFields[1]);
            tmpFrom = Double.valueOf(RateFields[2]);
            tmpTo = Double.valueOf(RateFields[3]);
            tmpBeat = Double.valueOf(RateFields[4]);
            tmpFactor = Double.valueOf(RateFields[5]);
            tmpChargeBase = Double.valueOf(RateFields[6]);

            // Add the entry to the cache
            addPriceModel(PriceModel, tmpTier, tmpFrom, tmpTo, tmpBeat, tmpFactor, tmpChargeBase);

            // Update to the log file
            if ((RatesLoaded % loadingLogNotificationStep) == 0)
            {
              message = "Rate Cache Data Loading: <" + RatesLoaded +
                    "> configurations loaded for <" + getSymbolicName() + "> from <" +
                    CacheDataFile + ">";
              OpenRate.getOpenRateFrameworkLog().info(message);
            }
          }
        }
      }
    }
    catch (IOException ex)
    {
      OpenRate.getOpenRateFrameworkLog().fatal(
            "Error reading input file <" + CacheDataFile +
            "> in record <" + RatesLoaded + ">. IO Error.");
    }
    finally
    {
      try
      {
        inFile.close();
      }
      catch (IOException ex)
      {
        OpenRate.getOpenRateFrameworkLog().error("Error closing input file <" + CacheDataFile +
                  ">", ex);
      }
    }

    OpenRate.getOpenRateFrameworkLog().info(
          "Rate Cache Data Loading completed. " + RatesLoaded +
          " configuration lines loaded from <" + CacheDataFile +
          ">");
  }

 /**
  * Load the data from the defined Data Source
  */
  @Override
  public void loadDataFromDB()
                      throws InitializationException
  {
    int     RatesLoaded = 0;
    String  PriceModel;
    double  tmpFrom;
    double  tmpTo;
    double  tmpBeat;
    double  tmpFactor;
    int     tmpTier;
    double  tmpChargeBase;

    // Find the location of the configuration file
    OpenRate.getOpenRateFrameworkLog().info("Starting Rate Cache Loading from DB");

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
      message = "Error performing SQL for retieving Rate Cache data";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,getSymbolicName());
    }

    // loop through the results for the customer login cache
    try
    {
      mrs.beforeFirst();

      while (mrs.next())
      {
        RatesLoaded++;
        PriceModel  = mrs.getString(1);
        tmpTier   = mrs.getInt(2);
        tmpFrom   = mrs.getDouble(3);
        tmpTo     = mrs.getDouble(4);
        tmpBeat   = mrs.getDouble(5);
        tmpFactor = mrs.getDouble(6);
        tmpChargeBase = mrs.getDouble(7);

        // Add the map
        addPriceModel(PriceModel, tmpTier, tmpFrom, tmpTo, tmpBeat, tmpFactor, tmpChargeBase);

        // Update to the log file
        if ((RatesLoaded % loadingLogNotificationStep) == 0)
        {
          message = "Rate Cache Data Loading: <" + RatesLoaded +
                "> configurations loaded for <" + getSymbolicName() + "> from <" +
                cacheDataSourceName + ">";
          OpenRate.getOpenRateFrameworkLog().info(message);
        }
      }
    }
    catch (SQLException ex)
    {
      message = "Error opening Search Map Data for <" + cacheDataSourceName + ">";
      OpenRate.getOpenRateFrameworkLog().fatal(message);
      throw new InitializationException(message,ex,getSymbolicName());
    }

    // Close down stuff
    DBUtil.close(mrs);
    DBUtil.close(StmtCacheDataSelectQuery);
    DBUtil.close(JDBCcon);

    OpenRate.getOpenRateFrameworkLog().info(
          "Rate Cache Data Loading completed. " + RatesLoaded +
          " configuration lines loaded from <" + cacheDataSourceName +
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
