/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2015.
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
    int PMIndex;
    ChargePacket tmpCPNew = null;
    RecordError tmpError;
    ArrayList<RUMMapCache.RUMMapEntry> tmpRUMMap;
    RUMMapCache.RUMMapEntry tmpRUMMapEntry;
    String priceGroup;
    ArrayList<ChargePacket> tmpCPList = new ArrayList<>();
    boolean replace = false;

    // Loop over the charge packets
    for (ChargePacket tmpCP : CurrentRecord.getChargePackets()) {
      // Get the price group for this charge packet
      if (tmpCP.Valid) {
        for (TimePacket tmpTZ : tmpCP.getTimeZones()) {
          priceGroup = tmpTZ.priceGroup;

        // create a charge packet for each RUM/Resource/price model tuple as located
          // in the RUM Map
          tmpRUMMap = RMC.getRUMMap(priceGroup);

          if (tmpRUMMap == null) {
            tmpError = new RecordError("ERR_PRICE_GROUP_MAP_NOT_FOUND", ErrorType.DATA_NOT_FOUND, getSymbolicName());
            CurrentRecord.addError(tmpError);

            // found an error - get out
            return false;
          }

        // if we are doing 1:1 price group:price model, we'll use the existing
          // charge packet, otherwise we have to do some cloning. Normally, we'll
          // be using 1:1
          if (tmpRUMMap.size() == 1) {
            // ************************** 1:1 case *********************************
            tmpRUMMapEntry = tmpRUMMap.get(0);

            // Get the value of the RUM
            tmpTZ.priceModel = tmpRUMMapEntry.PriceModel;
            tmpCP.rumName = tmpRUMMapEntry.RUM;
            tmpCP.rumQuantity = CurrentRecord.getRUMValue(tmpCP.rumName);

            tmpCP.resource = tmpRUMMapEntry.Resource;
            tmpCP.resCounter = tmpRUMMapEntry.ResourceCounter;
            tmpCP.ratingType = tmpRUMMapEntry.RUMType;

          // Fill the RUM value
            // get the rating type
            switch (tmpRUMMapEntry.RUMType) {
              case 1: {
                tmpCP.ratingTypeDesc = "FLAT";
                break;
              }
              case 2: {
                // Tiered Rating
                tmpCP.ratingTypeDesc = "TIERED";
                break;
              }
              case 3: {
                tmpCP.ratingTypeDesc = "THRESHOLD";
                break;
              }
              case 4: {
                // Event Rating
                tmpCP.ratingTypeDesc = "EVENT";
                break;
              }
            }

            // Add to the list of processed CPs
            tmpCPList.add(tmpCP);
          } else {
            // ************************ 1:many case ******************************
            for (PMIndex = 0; PMIndex < tmpRUMMap.size(); PMIndex++) {
              tmpRUMMapEntry = tmpRUMMap.get(PMIndex);

            // Copy the CP over - we do this for each model in the group
              // as we will be performing rating on each of them
              // Note that we create a new list of cloned charge packets, and
              // don't try to re-use the original ones. This saves a loop of
              // preparation and then rating. I think that it's quicker this
              // way, but there's the potential to do some timing/tuning here
              replace = true;
              tmpCPNew = tmpCP.Clone();

              // Get the value of the RUM
              tmpCPNew.rumName = tmpRUMMapEntry.RUM;
              tmpCPNew.rumQuantity = CurrentRecord.getRUMValue(tmpCP.rumName);
              tmpCPNew.resource = tmpRUMMapEntry.Resource;
              tmpCPNew.resCounter = tmpRUMMapEntry.ResourceCounter;
              tmpCPNew.ratingType = tmpRUMMapEntry.RUMType;

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
            }

            tmpCPList.add(tmpCPNew);
          }
        }
      } else {
        // skip the packet
        tmpCPList.add(tmpCP);
      }
    }

    // replace the list of unprepared packets with the prepared ones
    if (replace) {
      CurrentRecord.replaceChargePackets(tmpCPList);
    }

    return true;
  }
}
