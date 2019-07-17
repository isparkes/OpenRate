
package OpenRate.process;

import OpenRate.cache.ICacheManager;
import OpenRate.cache.RUMCPRateCache;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.*;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;

/**
 * This class provides the abstract base for the more complex rating plug in.
 * The implementation class should not have to do much more than call the
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
public abstract class AbstractRUMCPRateCalc extends AbstractRateCalc {

  // This is the object will be using the find the cache manager
  private ICacheManager CMRR = null;

  // The zone model object
  private RUMCPRateCache RRC;

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
    if (CMRR.get(CacheObjectName) instanceof RUMCPRateCache) {
      RRC = (RUMCPRateCache) CMRR.get(CacheObjectName);

      if (RRC == null) {
        message = "Could not find cache entry for <" + CacheObjectName + ">";
        throw new InitializationException(message, getSymbolicName());
      }
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
   * performRating is the main call for the RUM basd rating. It performs the
   * rating operations on a record of type "RatingRecord".
   *
   * @param CurrentRecord
   * @return True if rating was performed without errors, otherwise false
   * @throws ProcessingException
   */
  public boolean performRating(RatingRecord CurrentRecord)
          throws ProcessingException {
    RecordError tmpError;

    // ***************************** Rating Evaluation**************************
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

// -----------------------------------------------------------------------------
// ----------------------- Start of utility functions --------------------------
// -----------------------------------------------------------------------------
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
  RatingResult rateCalculateTiered(String priceModel, double valueToRate, double valueOffset, long CDRDate, boolean breakdown)
          throws ProcessingException {
    ArrayList<RateMapEntry> tmpRateModel;
    RatingResult tmpRatingResult;

    // Look up the rate model to use
    tmpRateModel = RRC.getPriceModel(priceModel);

    // perform the rating using the selected rate model
    tmpRatingResult = performRateEvaluationTiered(priceModel, tmpRateModel, valueToRate, valueOffset, CDRDate, breakdown);

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
  RatingResult rateCalculateThreshold(String priceModel, double valueToRate, double valueOffset, long CDRDate, boolean breakdown)
          throws ProcessingException {
    ArrayList<RateMapEntry> tmpRateModel;
    RatingResult tmpRatingResult;

    // Look up the rate model to use
    tmpRateModel = RRC.getPriceModel(priceModel);

    // perform the rating using the selected rate model
    tmpRatingResult = performRateEvaluationThreshold(priceModel, tmpRateModel, valueToRate, valueOffset, CDRDate, breakdown);

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
  RatingResult rateCalculateFlat(String priceModel, double valueToRate, long CDRDate, boolean breakdown)
          throws ProcessingException {
    ArrayList<RateMapEntry> tmpRateModel;
    RatingResult tmpRatingResult;

    // Look up the rate model to use
    tmpRateModel = RRC.getPriceModel(priceModel);

    // perform the rating using the selected rate model
    tmpRatingResult = performRateEvaluationFlat(priceModel, tmpRateModel, valueToRate, CDRDate, breakdown);

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
  RatingResult rateCalculateEvent(String priceModel, double valueToRate, long CDRDate, boolean breakdown)
          throws ProcessingException {
    ArrayList<RateMapEntry> tmpRateModel;
    RatingResult tmpRatingResult;

    // Look up the rate model to use
    tmpRateModel = RRC.getPriceModel(priceModel);

    // perform the rating using the selected rate model
    tmpRatingResult = performRateEvaluationEvent(priceModel, tmpRateModel, (long) valueToRate, CDRDate, breakdown);

    // return the rating result
    return tmpRatingResult;
  }
}
