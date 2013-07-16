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

package OpenRate.process;

import OpenRate.cache.ICacheManager;
import OpenRate.cache.RUMRateCache;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.*;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;

/**
 * Please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=RUM_Rate_Calculation'>click here</a> to go to wiki page.
 * <br>
 * <p>
 * This class provides the abstract base for the more complex rating plug in.
 * The implementation class should not have to do much more than call the
 * "PerformRating", after having set the appropriate RUM values.
 *
 * The rating is performed on the Charge Packets that should already be
 * and should have the "priceGroup" field filled with an appropriate value.
 * Usually the priceGroup value is retrieved using the zoning and timing
 * results. Note that the number of Charge Packets can (and often will) increase
 * during the rating, because we may have to impact multiple resources (a
 * charge packet will be created for each of these resource impacts (in the case
 * that multiple resources have been impacted.
 *
 * No roll-up of charges is performed in this module. You can use the module
 * "GatherRUMImpacts" to collect and create a summary of the CP impacts.
 *
 * You can obtain a rating breakdown (which provides exact details of the steps
 * and tiers used to calculate the charge) by enabling the standard rating record
 * field "CreateBreakdown" boolean value to true.
 */
public abstract class AbstractRUMRateCalc extends AbstractRateCalc
{
  // This is the object will be using the find the cache manager
  private ICacheManager CMRR = null;

  // The zone model object
  private RUMRateCache RRC;

  // this tells us whether to deal with the exception ourself, pr pass it to the
  // parent module for handling
  private boolean reportExceptions = false;

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited Plug In functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * Initialise the module. Called during pipeline creation to initialise:
  *  - Configuration properties that are defined in the properties file.
  *  - The references to any cache objects that are used in the processing
  *  - The symbolic name of the module
  *
  * @param PipelineName The name of the pipeline this module is in
  * @param ModuleName The name of this module in the pipeline
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String PipelineName, String ModuleName)
            throws InitializationException
  {
    String CacheObjectName;

    // Do the inherited work, e.g. setting the symbolic name etc
    super.init(PipelineName,ModuleName);

    // Get the cache object reference
    CacheObjectName = PropertyUtils.getPropertyUtils().getPluginPropertyValue(PipelineName,
                                                           ModuleName,
                                                           "DataCache");
    CMRR = CacheFactory.getGlobalManager(CacheObjectName);

    if (CMRR == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Load up the mapping arrays, but only if we are the right type. This
    // allows us to build up ever more complex rating models, matching the
    // right model to the right cache
    if (CMRR.get(CacheObjectName) instanceof RUMRateCache)
    {
      RRC = (RUMRateCache)CMRR.get(CacheObjectName);

      if (RRC == null)
      {
        message = "Could not find cache entry for <" + CacheObjectName + ">";
        throw new InitializationException(message,getSymbolicName());
      }
    }
    else
    {
      message = "<" + CacheObjectName + "> is not an instance of RUMRateCache. Aborting.";
      throw new InitializationException(message,getSymbolicName());
    }
  }

  @Override
  public IRecord procHeader(IRecord r)
  {
    return r;
  }

  @Override
  public IRecord procTrailer(IRecord r)
  {
    return r;
  }

// -----------------------------------------------------------------------------
// ---------------------- Start of exposed functions ---------------------------
// -----------------------------------------------------------------------------

  /**
   * PerformRating is the main call for the RUM based rating. It performs the
   * rating operations on a record of type "RatingRecord".
   *
   * For each charge packet in the record, the price group is inspected, and
   * compared against the RUM map for that price group. Recall that each price
   * group can be mapped to multiple price model/RUM/Resource tuples, so in the
   * case that a single price group is mapped to multiple price models, these
   * are expanded (by creating additional charge packets) before rating.
   *
   * Rating is then performed on the charge packets, treating each charge
   * packet individually.
   *
   * @param CurrentRecord The record to rate
   * @return True if rating was performed without errors, otherwise false
   * @throws ProcessingException
   */
  public boolean PerformRating(RatingRecord CurrentRecord) throws ProcessingException
  {
    int PMIndex;
    int Index;
    ChargePacket tmpCP;
    ChargePacket tmpCPNew;
    RecordError tmpError;
    ArrayList<RUMRateCache.RUMMapEntry> tmpRUMMap;
    RUMRateCache.RUMMapEntry tmpRUMMapEntry;
    double RUMValue;
    ArrayList<ChargePacket> tmpCPList = new ArrayList<>();
    boolean CPUpdated = false;
    RatingResult tmpRatingResult;

    // Rate all of the charge packets that are to be rated - loop through the
    // charge packets and apply the time zone results
    for (Index = 0 ; Index < CurrentRecord.getChargePacketCount() ; Index++)
    {
      tmpCP = CurrentRecord.getChargePacket(Index);

      if (tmpCP.Valid)
      {
        try
        {
           //Use the rateCalculateDuration method defined in AbstractRateCalc to
           //calculate the price
          if (tmpCP.priceGroup != null)
          {
            if (!(tmpCP.priceGroup.equals("NOMATCH")))
            {
              // Get the price models for this RUM group
              tmpRUMMap = RRC.getRUMMap(tmpCP.priceGroup);

              if (tmpRUMMap == null)
              {
                if (reportExceptions == false)
                {
                  tmpError = new RecordError("ERR_PRICE_GROUP_MAP_NOT_FOUND",ErrorType.DATA_NOT_FOUND, getSymbolicName());
                  CurrentRecord.addError(tmpError);

                  return false;
                }
                else
                {
                  throw new ProcessingException ("Price group map not found for <"+tmpCP.priceGroup+">",getSymbolicName());
                }
              }

              // Calculate the price of the charge packet
              for (PMIndex = 0 ; PMIndex < tmpRUMMap.size() ; PMIndex++)
              {
                tmpRUMMapEntry = tmpRUMMap.get(PMIndex);

                // Copy the CP over - we do this for each model in the group
                // as we will be performing rating on each of them
                // Note that we create a new list of cloned charge packets, and
                // don't try to re-use the original ones. This saves a loop of
                // preparation and then rating. I think that it's quicker this
                // way, but there's the potential to do some timing/tuning here
                tmpCPNew = tmpCP.Clone();
                tmpCPList.add(tmpCPNew);
                CPUpdated = true;

                // Get the value of the RUM
                RUMValue = CurrentRecord.getRUMValue(tmpRUMMapEntry.RUM);
                tmpCPNew.priceModel = tmpRUMMapEntry.PriceModel;
                tmpCPNew.RUMQuantity = RUMValue;
                tmpCPNew.RUMName = tmpRUMMapEntry.RUM;
                tmpCPNew.resource = tmpRUMMapEntry.Resource;
                tmpCPNew.ResCounter = tmpRUMMapEntry.ResourceCounter;
                tmpCPNew.ratingType  = tmpRUMMapEntry.RUMType;


                // perform the rating
                switch (tmpRUMMapEntry.RUMType)
                {
                  case ChargePacket.RATING_TYPE_FLAT:
                    {
                      // Flat Rating
                      tmpRatingResult = rateCalculateFlat(tmpRUMMapEntry.PriceModel, RUMValue, CurrentRecord.UTCEventDate, CurrentRecord.CreateBreakdown);
                      tmpCPNew.chargedValue += tmpRatingResult.RatedValue;
                      tmpCPNew.breakDown = tmpRatingResult.breakdown;
                      tmpCPNew.ratingTypeDesc = "FLAT";
                      if (tmpRUMMapEntry.ConsumeRUM)
                      {
                        CurrentRecord.updateRUMValue(tmpRUMMapEntry.RUM,-tmpRatingResult.RUMUsed);
                      }
                      break;
                    }
                  case ChargePacket.RATING_TYPE_TIERED:
                    {
                      // Tiered Rating
                      tmpRatingResult = rateCalculateTiered(tmpRUMMapEntry.PriceModel, RUMValue, CurrentRecord.UTCEventDate, CurrentRecord.CreateBreakdown);
                      tmpCPNew.chargedValue += tmpRatingResult.RatedValue;
                      tmpCPNew.breakDown = tmpRatingResult.breakdown;
                      tmpCPNew.ratingTypeDesc = "TIERED";
                      if (tmpRUMMapEntry.ConsumeRUM)
                      {
                        CurrentRecord.updateRUMValue(tmpRUMMapEntry.RUM,-tmpRatingResult.RUMUsed);
                      }
                      break;
                    }
                  case ChargePacket.RATING_TYPE_THRESHOLD:
                    {
                      // Threshold Rating
                      tmpRatingResult = rateCalculateThreshold(tmpRUMMapEntry.PriceModel, RUMValue, CurrentRecord.UTCEventDate, CurrentRecord.CreateBreakdown);
                      tmpCPNew.chargedValue += tmpRatingResult.RatedValue;
                      tmpCPNew.breakDown = tmpRatingResult.breakdown;
                      tmpCPNew.ratingTypeDesc = "THRESHOLD";
                      if (tmpRUMMapEntry.ConsumeRUM)
                      {
                        CurrentRecord.updateRUMValue(tmpRUMMapEntry.RUM,-tmpRatingResult.RUMUsed);
                      }
                      break;
                    }
                  case ChargePacket.RATING_TYPE_EVENT:
                    {
                      // Event Rating
                      tmpRatingResult = rateCalculateEvent(tmpRUMMapEntry.PriceModel, RUMValue, CurrentRecord.UTCEventDate, CurrentRecord.CreateBreakdown);
                      tmpCPNew.chargedValue += tmpRatingResult.RatedValue;
                      tmpCPNew.breakDown = tmpRatingResult.breakdown;
                      tmpCPNew.ratingTypeDesc = "EVENT";
                      if (tmpRUMMapEntry.ConsumeRUM)
                      {
                        CurrentRecord.updateRUMValue(tmpRUMMapEntry.RUM,-tmpRatingResult.RUMUsed);
                      }
                      break;
                    }
                }
              }
            }
            else
            {
              // Fill the default fields in the CP
              tmpCP.RUMName = "";
              tmpCP.priceModel = "";
              tmpCP.resource = "";
              tmpCP.Valid = false;
            }
          }
          else
          {
            // we do not have a price group, set the cp invalid
            tmpCP.Valid = false;
          }
        }
        catch (ProcessingException pe)
        {
          // Log the error
          getPipeLog().error("RUM Rating exception <" + pe.getMessage() + ">");

          if (reportExceptions == false)
          {
            // Only error if this is a base packet
            if (tmpCP.priority == 0)
            {
              tmpError = new RecordError("ERR_RUM_RATING",ErrorType.DATA_NOT_FOUND, getSymbolicName());
              CurrentRecord.addError(tmpError);

              return false;
            }
          }
          else
          {
            throw new ProcessingException (pe,getSymbolicName());
          }
        }
      }
      else
      {
        // just add the packet
        tmpCPList.add(tmpCP);
      }
    }

    // write back the results if we have changed the CPs
    if (CPUpdated)
    {
      CurrentRecord.replaceChargePackets(tmpCPList);
    }

    return true;
  }

// -----------------------------------------------------------------------------
// ----------------------- Start of utility functions --------------------------
// -----------------------------------------------------------------------------

 /**
  * Set the state of the exception reporting. True means that we let the parent
  * module deal with it, false means that we deal with it ourselves.
  *
  * @param NewValue
  */
  public void setExceptionReporting(boolean NewValue)
  {
    reportExceptions = NewValue;
  }

 /**
  * This function does the rating based on the chosen price model and the
  * RUM (Rateable Usage Metric) value. The model processes all of the tiers
  * up to the value of the RUM, calculating the cost for each tier and summing
  * the tier costs. This is different to the "threshold" mode where all of the
  * RUM is used from the tier that is reached.
  *
  * @param  priceModel The price model to use
  * @param  valueToRate the duration that should be rated in seconds
  * @return the price for the rated record
  * @throws OpenRate.exception.ProcessingException
  */
  RatingResult rateCalculateTiered(String priceModel, double valueToRate, long CDRDate, boolean BreakDown)
    throws ProcessingException
  {
    ArrayList<RateMapEntry>  tmpRateModel;
    RatingResult tmpRatingResult;

    // Look up the rate model to use
    tmpRateModel = RRC.getPriceModel(priceModel);

    // perform the rating using the selected rate model
    tmpRatingResult = performRateEvaluationTiered(priceModel, tmpRateModel, valueToRate, CDRDate, BreakDown);

    // return the rating result
    return tmpRatingResult;
  }

 /**
  * This function does the rating based on the chosen price model and the
  * RUM (Rateable Usage Metric) value. The model locates the correct tier to
  * use and then rates all of the RUM according to that tier. This is different
  * to the "tiered" mode, where the individual contributing tier costs are
  * calculated and then summed.
  *
  * @param  priceModel The price model to use
  * @param  valueToRate the duration that should be rated in seconds
  * @return the price for the rated record
  * @throws OpenRate.exception.ProcessingException
  */
  RatingResult rateCalculateThreshold(String priceModel, double valueToRate, long CDRDate, boolean BreakDown)
    throws ProcessingException
  {
    ArrayList<RateMapEntry>  tmpRateModel;
    RatingResult tmpRatingResult;

    // Look up the rate model to use
    tmpRateModel = RRC.getPriceModel(priceModel);

    // perform the rating using the selected rate model
    tmpRatingResult = performRateEvaluationThreshold(priceModel, tmpRateModel, valueToRate, CDRDate, BreakDown);

    // return the rating result
    return tmpRatingResult;
  }

 /**
  * This function does the rating based on the chosen price model and the
  * RUM (Rateable Usage Metric) value. It is a simplified version of the
  * "tiered" model that just does a multiplication of valueToRate*Rate,
  * without having to calculate tiers and beats.
  *
  * @param  priceModel The price model to use
  * @param valueToRate the value that we are rating
  * @return the price for the rated record
  * @throws OpenRate.exception.ProcessingException
  */
  RatingResult rateCalculateFlat(String priceModel, double valueToRate, long CDRDate, boolean BreakDown)
    throws ProcessingException
  {
    ArrayList<RateMapEntry>  tmpRateModel;
    RatingResult tmpRatingResult;

    // Look up the rate model to use
    tmpRateModel = RRC.getPriceModel(priceModel);

    // perform the rating using the selected rate model
    tmpRatingResult = performRateEvaluationFlat(priceModel, tmpRateModel, valueToRate, CDRDate, BreakDown);

    // return the rating result
    return tmpRatingResult;
  }

 /**
  * This function does the rating based on the chosen price model and the
  * RUM (Rateable Usage Metric) value. It is a simplified version of the
  * "tiered" model that just returns the event price.
  *
  * @param  priceModel The price model to use
  * @return the price for the rated record
  * @throws OpenRate.exception.ProcessingException
  */
  RatingResult rateCalculateEvent(String priceModel, double valueToRate, long CDRDate, boolean BreakDown)
    throws ProcessingException
  {
    ArrayList<RateMapEntry>  tmpRateModel;
    RatingResult tmpRatingResult;

    // Look up the rate model to use
    tmpRateModel = RRC.getPriceModel(priceModel);

    // perform the rating using the selected rate model
    tmpRatingResult = performRateEvaluationEvent(priceModel, tmpRateModel, (long) valueToRate, CDRDate, BreakDown);

    // return the rating result
    return tmpRatingResult;
  }

  /**
  * This method is used to calculate the number of RUM units (e.g. seconds)
  * which can be purchased for the available credit. The credit is calculated
  * from the difference between the current balance and the credit limit. In
  * pre-paid scenarios, the current balance will tend to be > 0 and the credit
  * limit will tend to be 0. In post paid scenarios, both values will tend to
  * be negative.
  *
  * The clever thing about this method is the fact that it uses the standard
  * rating price model in order to arrive at the value, simplifying greatly the
  * management of pre-paid balances.
  *
  * This method uses the TIERED rating model.
  *
  * @param priceModel The price model to use
  * @param availableBalance The current balance the user has available to them, positive
  * @param CDRDate The date to rate at
  * @return The number of RUM units that can be purchased for the available balance
  * @throws ProcessingException
  */
    @Override
  public double authCalculateTiered(String priceModel, double availableBalance, long CDRDate)
    throws ProcessingException
  {
    if(availableBalance <= 0){
       return 0;
    }

      ArrayList<RateMapEntry>  tmpRateModel;

      double tmpcalculationResult;

    // Look up the rate model to use
    tmpRateModel = RRC.getPriceModel(priceModel);

    // perform the calculation using the selected rate model
    tmpcalculationResult = performAuthEvaluationTiered(priceModel, tmpRateModel, availableBalance, CDRDate);

    return tmpcalculationResult;
  }

  /**
  * This method is used to calculate the number of RUM units (e.g. seconds)
  * which can be purchased for the available credit. The credit is calculated
  * from the difference between the current balance and the credit limit. In
  * pre-paid scenarios, the current balance will tend to be > 0 and the credit
  * limit will tend to be 0. In post paid scenarios, both values will tend to
  * be negative.
  *
  * The clever thing about this method is the fact that it uses the standard
  * rating price model in order to arrive at the value, simplifying greatly the
  * management of pre-paid balances.
  *
  * This method uses the THRESHOLD rating model.
  *
  * @param priceModel The price model to use
  * @param availableBalance The current balance the user has available to them, positive
  * @param CDRDate The date to rate at
  * @return The number of RUM units that can be purchased for the available balance
  * @throws ProcessingException
  */
  @Override
  public double authCalculateThreshold(String priceModel, double availableBalance, long CDRDate)
    throws ProcessingException
  {
    if(availableBalance <= 0){
       return 0;
    }

      ArrayList<RateMapEntry>  tmpRateModel;

      double tmpcalculationResult;

    // Look up the rate model to use
    tmpRateModel = RRC.getPriceModel(priceModel);

    // perform the calculation using the selected rate model
    tmpcalculationResult = performAuthEvaluationThreshold(priceModel, tmpRateModel, availableBalance, CDRDate);

    return tmpcalculationResult;
  }
}
