package OpenRate.process;

import OpenRate.cache.ICacheManager;
import OpenRate.cache.RUMMapCache;
import OpenRate.exception.InitializationException;
import OpenRate.record.*;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;

/**
 * This class provides the abstract base for the more complex rating plug in.
 * The implementation class should not have to do much more than call the
 * "PerformRating", after having set the appropriate RUM values.
 *
 * The rating is performed on the Charge Packets that should already be and
 * should have the "priceGroup" field filled with an appropriate value. Usually
 * the priceGroup value is retrieved using the zoning and timing results. Note
 * that the number of Charge Packets can (and often will) increase during the
 * rating, because we may have to impact multiple resources (a charge packet
 * will be created for each of these resource impacts (in the case that multiple
 * resources have been impacted.
 *
 * No roll-up of charges is performed in this module. You can use the module
 * "GatherRUMImpacts" to collect and create a summary of the CP impacts.
 *
 * You can obtain a rating breakdown (which provides exact details of the steps
 * and tiers used to calculate the charge) by enabling the standard rating
 * record field "CreateBreakdown" boolean value to true.
 */
public abstract class AbstractRUMMap extends AbstractPlugIn {

  // This is the object will be using the find the cache manager
  private ICacheManager CMRM = null;

  // The zone model object
  private RUMMapCache RMC;

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
    CMRM = CacheFactory.getGlobalManager(CacheObjectName);

    if (CMRM == null) {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message, getSymbolicName());
    }

    // Load up the mapping arrays, but only if we are the right type. This
    // allows us to build up ever more complex rating models, matching the
    // right model to the right cache
    if (CMRM.get(CacheObjectName) instanceof RUMMapCache) {
      RMC = (RUMMapCache) CMRM.get(CacheObjectName);

      if (RMC == null) {
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
   * Evaluate the price group of existing charge packets, completing the
   * priceModel/RUM/Resource information in each of the packets found. Note that
   * because a price group can contain multiple price models, the number of
   * charge packets after the call may have been increased in order to perform
   * the priceGroup -> priceModel/RUM/Resource expansion.
   *
   * This method will only work on records of a "RatingRecord" inheritance.
   *
   * This method sets all of the RUM, price group, price model and resource
   * elements of a charge packet, working on a charge packet that has already
   * been created, and which has a priceGroup already defined in it. Thus, this
   * method is intended for moderately simple scenarios where the price group is
   * available before other elements of the rating, but where the work has been
   * performed on a charge packet. The results of this method can be rated
   * directly with the RUMCPRateCalc:PerformRating method.
   *
   * In the case of error, the error is added directly to the record.
   *
   * @param CurrentRecord The record we are working on
   * @return true if the processing was performed without error, otherwise false
   */
  public boolean evaluateRUMPriceGroup(RatingRecord CurrentRecord) {
    RecordError tmpError;
    ArrayList<RUMMapCache.RUMMapEntry> tmpRUMMap;
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
          tmpRUMMap = RMC.getRUMMap(tmpTZ.priceGroup);

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
            RUMMapCache.RUMMapEntry tmpRUMMapEntry = tmpRUMMap.get(0);

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
            for (RUMMapCache.RUMMapEntry tmpRUMMapEntry : tmpRUMMap) {

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

    return true;
  }
}
