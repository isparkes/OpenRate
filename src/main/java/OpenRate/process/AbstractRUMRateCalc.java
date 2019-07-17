
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
 * Please
 * <a target='new' href='http://www.open-rate.com/wiki/index.php?title=RUM_Rate_Calculation'>click
 * here</a> to go to wiki page.
 * <br>
 * <p>
 This class provides the abstract base for the more complex rating plug in.
 The implementation class should not have to do much more than call the
 "performRating", after having set the appropriate RUM values.

 The rating is performed on the Charge Packets that should already be and
 should have the "priceGroup" field filled with an appropriate value. Usually
 the priceGroup value is retrieved using the zoning and timing results. Note
 that the number of Charge Packets can (and often will) increase during the
 rating, because we may have to impact multiple resources (a charge packet
 will be created for each of these resource impacts (in the case that multiple
 resources have been impacted.

 No roll-up of charges is performed in this module. You can use the module
 "GatherRUMImpacts" to collect and create a summary of the CP impacts.

 You can obtain a rating breakdown (which provides exact details of the steps
 and tiers used to calculate the charge) by enabling the standard rating
 record field "createBreakdown" boolean value to true.
 */
public abstract class AbstractRUMRateCalc extends AbstractRateCalc {

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
   * Initialise the module. Called during pipeline creation to initialise: -
   * Configuration properties that are defined in the properties file. - The
   * references to any cache objects that are used in the processing - The
   * symbolic name of the module
   *
   * @param PipelineName The name of the pipeline this module is in
   * @param ModuleName The name of this module in the pipeline
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void init(String PipelineName, String ModuleName)
          throws InitializationException {
    String CacheObjectName;

    // Do the inherited work, e.g. setting the symbolic name etc
    super.init(PipelineName, ModuleName);

    // Get the cache object reference
    CacheObjectName = PropertyUtils.getPropertyUtils().getPluginPropertyValue(PipelineName,
            ModuleName,
            "DataCache");
    CMRR = CacheFactory.getGlobalManager(CacheObjectName);

    if (CMRR == null) {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message, getSymbolicName());
    }

    // Load up the mapping arrays, but only if we are the right type. This
    // allows us to build up ever more complex rating models, matching the
    // right model to the right cache
    if (CMRR.get(CacheObjectName) instanceof RUMRateCache) {
      RRC = (RUMRateCache) CMRR.get(CacheObjectName);

      if (RRC == null) {
        message = "Could not find cache entry for <" + CacheObjectName + ">";
        throw new InitializationException(message, getSymbolicName());
      }
    } else {
      message = "<" + CacheObjectName + "> is not an instance of RUMRateCache. Aborting.";
      throw new InitializationException(message, getSymbolicName());
    }
  }

  @Override
  public IRecord procHeader(IRecord r) {
    return r;
  }

  @Override
  public IRecord procTrailer(IRecord r) {
    return r;
  }

// -----------------------------------------------------------------------------
// ---------------------- Start of exposed functions ---------------------------
// -----------------------------------------------------------------------------
  /**
   * performRating is the main call for the RUM based rating. It performs the
   * rating operations on a record of type "RatingRecord".
   *
   * For each charge packet in the record, the price group is inspected, and
   * compared against the RUM map for that price group. Recall that each price
   * group can be mapped to multiple price model/RUM/Resource tuples, so in the
   * case that a single price group is mapped to multiple price models, these
   * are expanded (by creating additional charge packets) before rating.
   *
   * Rating is then performed on the charge packets, treating each charge packet
   * individually.
   *
   * @param CurrentRecord The record to rate
   * @return True if rating was performed without errors, otherwise false
   * @throws ProcessingException
   */
  public boolean performRating(RatingRecord CurrentRecord) throws ProcessingException {
    RecordError tmpError;
    ArrayList<RUMRateCache.RUMMapEntry> tmpRUMMap;
    ArrayList<ChargePacket> tmpCPList = new ArrayList<>();

    // ****************************** RUM Expansion ****************************
    for (ChargePacket tmpCP : CurrentRecord.getChargePackets()) {
      // Used for building rating chains
      ChargePacket lastCP = null;

      if (tmpCP.Valid) {
        for (TimePacket tmpTZ : tmpCP.getTimeZones()) {

          if (tmpTZ.priceGroup == null) {
            tmpError = new RecordError("ERR_PRICE_GROUP_NOT_FOUND", ErrorType.DATA_NOT_FOUND, getSymbolicName());
            CurrentRecord.addError(tmpError);

            // found an error - get out
            return false;
          }

          // create a charge packet for each RUM/Resource/price model tuple as located
          // in the RUM Map
          tmpRUMMap = RRC.getRUMMap(tmpTZ.priceGroup);

          if (tmpRUMMap == null) {
            tmpError = new RecordError("ERR_PRICE_GROUP_MAP_NOT_FOUND", ErrorType.DATA_NOT_FOUND, getSymbolicName());
            CurrentRecord.addError(tmpError);

            // found an error - get out
            return false;
          }

          // if we are doing 1:1 price group:price model, we'll use the existing
          // charge packet, otherwise we have to do some clSetLicense.groovyoning. Normally, we'll
          // be using 1:1
          if (tmpRUMMap.size() == 1) {
            // ************************** 1:1 case *********************************
            RUMRateCache.RUMMapEntry tmpRUMMapEntry = tmpRUMMap.get(0);

            // Copy the CP over - we do this for each model in the group
            // as we will be performing rating on each of them
            // Note that we create a new list of cloned charge packets, and
            // don't try to re-use the original ones. This saves a loop of
            // preparation and then rating. I think that it's quicker this
            // way, but there's the potential to do some timing/tuning here
            ChargePacket tmpCPNew = tmpCP.shallowClone();

            // Set up the rating chain
            if (lastCP != null) {
              lastCP.nextChargePacket = tmpCPNew;
              tmpCPNew.previousChargePacket = lastCP;
            }

            // clone the TZ packet we are working on
            TimePacket tmpTZNew = tmpTZ.Clone();

            // Get the value of the RUM
            tmpTZNew.priceModel = tmpRUMMapEntry.PriceModel;
            tmpCPNew.rumName = tmpRUMMapEntry.RUM;
            tmpCPNew.rumQuantity = CurrentRecord.getRUMValue(tmpCP.rumName);

            tmpCPNew.resource = tmpRUMMapEntry.Resource;
            tmpCPNew.resCounter = tmpRUMMapEntry.ResourceCounter;
            tmpCPNew.ratingType = tmpRUMMapEntry.RUMType;
            tmpCPNew.consumeRUM = tmpRUMMapEntry.ConsumeRUM;
            tmpCPNew.addTimeZone(tmpTZNew);

            // Fill the RUM value
            // get the rating type
            switch (tmpRUMMapEntry.RUMType) {
              case 1: {
                tmpCPNew.ratingTypeDesc = "FLAT";
                break;
              }
              case 2: {
                // Tiered Rating
                tmpCPNew.ratingTypeDesc = "TIERED";
                break;
              }
              case 3: {
                tmpCPNew.ratingTypeDesc = "THRESHOLD";
                break;
              }
              case 4: {
                // Event Rating
                tmpCPNew.ratingTypeDesc = "EVENT";
                break;
              }
            }

            // Add to the list of processed CPs (in case we switch to a replace 
            // mode in a later CP/TZ)
            tmpCPList.add(tmpCPNew);

            // Set the rating chain up for this packet
            lastCP = tmpCPNew;
          } else {
            // ************************ 1:many case ******************************
            for (RUMRateCache.RUMMapEntry tmpRUMMapEntry : tmpRUMMap) {

              // Copy the CP over - we do this for each model in the group
              // as we will be performing rating on each of them
              // Note that we create a new list of cloned charge packets, and
              // don't try to re-use the original ones. This saves a loop of
              // preparation and then rating. I think that it's quicker this
              // way, but there's the potential to do some timing/tuning here
              ChargePacket tmpCPNew = tmpCP.shallowClone();

              // clone the TZ packet we are working on
              TimePacket tmpTZNew = tmpTZ.Clone();

              // Get the value of the RUM
              tmpTZNew.priceModel = tmpRUMMapEntry.PriceModel;
              tmpCPNew.rumName = tmpRUMMapEntry.RUM;
              tmpCPNew.rumQuantity = CurrentRecord.getRUMValue(tmpCP.rumName);
              tmpCPNew.resource = tmpRUMMapEntry.Resource;
              tmpCPNew.resCounter = tmpRUMMapEntry.ResourceCounter;
              tmpCPNew.ratingType = tmpRUMMapEntry.RUMType;
              tmpCPNew.addTimeZone(tmpTZNew);

              // get the rating type
              switch (tmpRUMMapEntry.RUMType) {
                case 1: {
                  tmpCPNew.ratingTypeDesc = "FLAT";
                  break;
                }
                case 2: {
                  // Tiered Rating
                  tmpCPNew.ratingTypeDesc = "TIERED";
                  break;
                }
                case 3: {
                  tmpCPNew.ratingTypeDesc = "THRESHOLD";
                  break;
                }
                case 4: {
                  // Event Rating
                  tmpCPNew.ratingTypeDesc = "EVENT";
                  break;
                }
              }
              tmpCPList.add(tmpCPNew);
            }
          }
        }
      } else {
        // skip the packet - just add it
        tmpCPList.add(tmpCP);
      }
    }

    // replace the list of unprepared packets with the prepared ones
    CurrentRecord.replaceChargePackets(tmpCPList);

    // ***************************** Rating Evaluation**************************
    // Rate all of the charge packets that are to be rated - loop through the
    // charge packets and apply the time zone results
    for (ChargePacket tmpCP : CurrentRecord.getChargePackets()) {
      if (tmpCP.Valid) {

        // Don't do Charge packets that are part of a chain
        if (tmpCP.previousChargePacket == null) {

          // get the RUM quantity
          double RUMValue = CurrentRecord.getRUMValue(tmpCP.rumName);

          // variables that we use to be able to manage beat rollover between time packets
          double rumExpectedCumulative = 0;
          double rumRoundedCumulative = 0;

          ChargePacket cpToRate = tmpCP;
          while (cpToRate != null) {
            for (TimePacket tmpTZ : cpToRate.getTimeZones()) {
              try {
                //Use the rateCalculateDuration method defined in AbstractRateCalc to
                //calculate the price
                if (tmpTZ.priceGroup != null) {
                  RatingResult tmpRatingResult;

                  // Get the rum value for the time zone according to the rounding rules
                  double thisZoneRUM = getRUMForTimeZone(RUMValue, rumRoundedCumulative, rumExpectedCumulative, cpToRate.timeSplitting, tmpTZ.duration, tmpTZ.totalDuration);
                  rumExpectedCumulative += tmpTZ.duration;

                  // perform the rating
                  switch (cpToRate.ratingType) {
                    case ChargePacket.RATING_TYPE_FLAT: {
                      // Flat Rating
                      tmpRatingResult = rateCalculateFlat(tmpTZ.priceModel, thisZoneRUM, CurrentRecord.utcEventDate, CurrentRecord.createBreakdown);
                      cpToRate.chargedValue += tmpRatingResult.RatedValue;
                      cpToRate.addBreakdown(tmpRatingResult.breakdown);
                      break;
                    }
                    case ChargePacket.RATING_TYPE_TIERED:
                    default: {
                      // Tiered Rating
                      tmpRatingResult = rateCalculateTiered(tmpTZ.priceModel, thisZoneRUM, rumRoundedCumulative, CurrentRecord.utcEventDate, CurrentRecord.createBreakdown);
                      cpToRate.chargedValue += tmpRatingResult.RatedValue;
                      cpToRate.addBreakdown(tmpRatingResult.breakdown);
                      break;
                    }
                    case ChargePacket.RATING_TYPE_THRESHOLD: {
                      // Threshold Rating
                      tmpRatingResult = rateCalculateThreshold(tmpTZ.priceModel, thisZoneRUM, rumRoundedCumulative, CurrentRecord.utcEventDate, CurrentRecord.createBreakdown);
                      cpToRate.chargedValue += tmpRatingResult.RatedValue;
                      cpToRate.addBreakdown(tmpRatingResult.breakdown);
                      break;
                    }
                    case ChargePacket.RATING_TYPE_EVENT: {
                      // Event Rating
                      tmpRatingResult = rateCalculateEvent(tmpTZ.priceModel, thisZoneRUM, CurrentRecord.utcEventDate, CurrentRecord.createBreakdown);
                      cpToRate.chargedValue += tmpRatingResult.RatedValue;
                      cpToRate.addBreakdown(tmpRatingResult.breakdown);
                      break;
                    }
                  }

                  if (cpToRate.consumeRUM) {
                    CurrentRecord.updateRUMValue(cpToRate.rumName, -tmpRatingResult.RUMUsed);
                  }

                  // Maintain a track of what we 
                  rumRoundedCumulative += tmpRatingResult.RUMUsedRounded;
                } else {
                  // we do not have a price group, set the cp invalid
                  cpToRate.Valid = false;
                }
              } catch (ProcessingException pe) {
                // Log the error
                getPipeLog().error("RUM Rating exception <" + pe.getMessage() + ">");

                if (reportExceptions == false) {
                  // Only error if this is a base packet
                  if (cpToRate.priority == 0) {
                    tmpError = new RecordError("ERR_RUM_RATING", ErrorType.DATA_NOT_FOUND, getSymbolicName());
                    CurrentRecord.addError(tmpError);

                    return false;
                  }
                } else {
                  throw new ProcessingException(pe, getSymbolicName());
                }
              }
            }

            // get next packet in the chain
            cpToRate = cpToRate.nextChargePacket;
          }
        }
      }
    }

    return true;
  }

  /**
   * Apply beat rounding to the RUM value if required by the time splitting.
   *
   * @param rawRUMAmount
   * @param rumRoundedCumulative
   * @param rumExpectedCumulative
   * @param timeSplittingMode
   * @param duration
   * @param totalDuration
   * @return
   */
  private double getRUMForTimeZone(double rawRUMAmount, double rumRoundedCumulative, double rumExpectedCumulative, int timeSplittingMode, int duration, int totalDuration) {
    switch (timeSplittingMode) {
      case AbstractRUMTimeMatch.TIME_SPLITTING_NO_CHECK:
      case AbstractRUMTimeMatch.TIME_SPLITTING_HOLIDAY: {
        // No processing needed
        return rawRUMAmount;
      }
      case AbstractRUMTimeMatch.TIME_SPLITTING_CHECK_SPLITTING: {
        // we accept the raw RUM value simply divided proportionally to the time
        return rawRUMAmount * (duration / (double) totalDuration);
      }
      case AbstractRUMTimeMatch.TIME_SPLITTING_CHECK_SPLITTING_BEAT_ROUNDING: {
        // Adjust the RUM for this zone to align to beat rounding
        double roundedRUMAmount = rawRUMAmount * ((duration - rumRoundedCumulative + rumExpectedCumulative) / (double) totalDuration);

        if (roundedRUMAmount < 0) {
          return 0;
        } else {
          return roundedRUMAmount;
        }
      }
      default: {
        // not expecting this, just accept the raw RUM value
        return rawRUMAmount;
      }
    }
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
  public void setExceptionReporting(boolean NewValue) {
    reportExceptions = NewValue;
  }

  /**
   * This function does the rating based on the chosen price model and the RUM
   * (Rateable Usage Metric) value. The model processes all of the tiers up to
   * the value of the RUM, calculating the cost for each tier and summing the
   * tier costs. This is different to the "threshold" mode where all of the RUM
   * is used from the tier that is reached.
   *
   * @param priceModel The price model to use
   * @param valueToRate the duration that should be rated in seconds
   * @return the price for the rated record
   * @throws OpenRate.exception.ProcessingException
   */
  RatingResult rateCalculateTiered(String priceModel, double valueToRate, double valueOffset, long CDRDate, boolean BreakDown)
          throws ProcessingException {
    ArrayList<RateMapEntry> tmpRateModel;
    RatingResult tmpRatingResult;

    // Look up the rate model to use
    tmpRateModel = RRC.getPriceModel(priceModel);

    // perform the rating using the selected rate model
    tmpRatingResult = performRateEvaluationTiered(priceModel, tmpRateModel, valueToRate, valueOffset, CDRDate, BreakDown);

    // return the rating result
    return tmpRatingResult;
  }

  /**
   * This function does the rating based on the chosen price model and the RUM
   * (Rateable Usage Metric) value. The model locates the correct tier to use
   * and then rates all of the RUM according to that tier. This is different to
   * the "tiered" mode, where the individual contributing tier costs are
   * calculated and then summed.
   *
   * @param priceModel The price model to use
   * @param valueToRate the duration that should be rated in seconds
   * @return the price for the rated record
   * @throws OpenRate.exception.ProcessingException
   */
  RatingResult rateCalculateThreshold(String priceModel, double valueToRate, double valueOffset, long CDRDate, boolean BreakDown)
          throws ProcessingException {
    ArrayList<RateMapEntry> tmpRateModel;
    RatingResult tmpRatingResult;

    // Look up the rate model to use
    tmpRateModel = RRC.getPriceModel(priceModel);

    // perform the rating using the selected rate model
    tmpRatingResult = performRateEvaluationThreshold(priceModel, tmpRateModel, valueToRate, valueOffset, CDRDate, BreakDown);

    // return the rating result
    return tmpRatingResult;
  }

  /**
   * This function does the rating based on the chosen price model and the RUM
   * (Rateable Usage Metric) value. It is a simplified version of the "tiered"
   * model that just does a multiplication of valueToRate*Rate, without having
   * to calculate tiers and beats.
   *
   * @param priceModel The price model to use
   * @param valueToRate the value that we are rating
   * @return the price for the rated record
   * @throws OpenRate.exception.ProcessingException
   */
  RatingResult rateCalculateFlat(String priceModel, double valueToRate, long CDRDate, boolean BreakDown)
          throws ProcessingException {
    ArrayList<RateMapEntry> tmpRateModel;
    RatingResult tmpRatingResult;

    // Look up the rate model to use
    tmpRateModel = RRC.getPriceModel(priceModel);

    // perform the rating using the selected rate model
    tmpRatingResult = performRateEvaluationFlat(priceModel, tmpRateModel, valueToRate, CDRDate, BreakDown);

    // return the rating result
    return tmpRatingResult;
  }

  /**
   * This function does the rating based on the chosen price model and the RUM
   * (Rateable Usage Metric) value. It is a simplified version of the "tiered"
   * model that just returns the event price.
   *
   * @param priceModel The price model to use
   * @return the price for the rated record
   * @throws OpenRate.exception.ProcessingException
   */
  RatingResult rateCalculateEvent(String priceModel, double valueToRate, long CDRDate, boolean BreakDown)
          throws ProcessingException {
    ArrayList<RateMapEntry> tmpRateModel;
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
   * @param availableBalance The current balance the user has available to them,
   * positive
   * @param CDRDate The date to rate at
   * @return The number of RUM units that can be purchased for the available
   * balance
   * @throws ProcessingException
   */
  @Override
  public double authCalculateTiered(String priceModel, double availableBalance, long CDRDate)
          throws ProcessingException {
    if (availableBalance <= 0) {
      return 0;
    }

    ArrayList<RateMapEntry> tmpRateModel;

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
   * @param availableBalance The current balance the user has available to them,
   * positive
   * @param CDRDate The date to rate at
   * @return The number of RUM units that can be purchased for the available
   * balance
   * @throws ProcessingException
   */
  @Override
  public double authCalculateThreshold(String priceModel, double availableBalance, long CDRDate)
          throws ProcessingException {
    if (availableBalance <= 0) {
      return 0;
    }

    ArrayList<RateMapEntry> tmpRateModel;

    double tmpcalculationResult;

    // Look up the rate model to use
    tmpRateModel = RRC.getPriceModel(priceModel);

    // perform the calculation using the selected rate model
    tmpcalculationResult = performAuthEvaluationThreshold(priceModel, tmpRateModel, availableBalance, CDRDate);

    return tmpcalculationResult;
  }
}
