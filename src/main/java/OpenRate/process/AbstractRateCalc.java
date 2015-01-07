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

import OpenRate.CommonConfig;
import OpenRate.cache.ICacheManager;
import OpenRate.cache.RateCache;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.IRecord;
import OpenRate.record.RateMapEntry;
import OpenRate.record.RatingBreakdown;
import OpenRate.record.RatingResult;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;

/**
 * Please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Rate_Calculation'>click here</a> to go to wiki page.
 * <br>
 * <p>
 * This class provides the abstract base for a rating plug in. A raw rate
 * object is retrieved from the RateCache object, and this class provides the
 * primitives required for performing rating.
 */
public abstract class AbstractRateCalc extends AbstractPlugIn
{
  // This is the object will be using the find the cache manager
  private ICacheManager CMR = null;

  // The zone model object
  private RateCache RC;

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
    CMR = CacheFactory.getGlobalManager(CacheObjectName);

    if (CMR == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Load up the mapping arrays, but only if we are the right type. This
    // allows us to build up ever more complex rating models, matching the
    // right model to the right cache
    if (CMR.get(CacheObjectName) instanceof RateCache)
    {
      RC = (RateCache)CMR.get(CacheObjectName);

      if (RC == null)
      {
        message = "Could not find cache entry for <" + CacheObjectName + ">";
        throw new InitializationException(message,getSymbolicName());
      }
    }
  }

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited Plug In functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * This is called when the synthetic Header record is encountered, and has the
  * meaning that the stream is starting.
  */
  @Override
  public IRecord procHeader(IRecord r)
  {
    return r;
  }

 /**
  * This is called when the synthetic trailer record is encountered, and has the
  * meaning that the stream is now finished.
  */
  @Override
  public IRecord procTrailer(IRecord r)
  {
    return r;
  }

  // -----------------------------------------------------------------------------
  // ------------------------ Start of utiity functions --------------------------
  // -----------------------------------------------------------------------------

 /**
  * This function does the rating based on the chosen price model and the
  * RUM (Rateable Usage Metric) value. The model processes all of the tiers
  * up to the value of the RUM, calculating the cost for each tier and summing
  * the tier costs. This is different to the "threshold" mode where all of the
  * RUM is used from the tier that is reached.
  *
  * @param  priceModel The price model to use
  * @param  valueToRate the duration that should be rated in seconds
  * @param CDRDate The date to use for price model version selection
  * @return the price for the rated record
  * @throws OpenRate.exception.ProcessingException
  */
  public double rateCalculateTiered(String priceModel, double valueToRate, long CDRDate)
    throws ProcessingException
  {
    ArrayList<RateMapEntry>  tmpRateModel;
    RatingResult tmpRatingResult;

    // Look up the rate model to use
    tmpRateModel = RC.getPriceModel(priceModel);

    // perform the rating using the selected rate model
    tmpRatingResult = performRateEvaluationTiered(priceModel, tmpRateModel, valueToRate, CDRDate, false);

    // return the rated value
    return tmpRatingResult.RatedValue;
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
  * @param CDRDate The date to use for price model version selection
  * @return the price for the rated record
  * @throws OpenRate.exception.ProcessingException
  */
  public double rateCalculateThreshold(String priceModel, double valueToRate, long CDRDate)
    throws ProcessingException
  {
    ArrayList<RateMapEntry>  tmpRateModel;
    RatingResult tmpRatingResult;

    // Look up the rate model to use
    tmpRateModel = RC.getPriceModel(priceModel);

    // perform the rating using the selected rate model
    tmpRatingResult = performRateEvaluationThreshold(priceModel, tmpRateModel, valueToRate, CDRDate, false);

    // return the rated value
    return tmpRatingResult.RatedValue;
  }

 /**
  * This function does the rating based on the chosen price model and the
  * RUM (Rateable Usage Metric) value. It is a simplified version of the
  * "tiered" model that just does a multiplication of valueToRate*Rate,
  * without having to calculate tiers. This of course does not support
  * singularity rating.
  *
  * @param  priceModel The price model to use
  * @param  valueToRate the duration that should be rated in seconds
  * @param CDRDate The date to use for price model version selection
  * @return the price for the rated record
  * @throws OpenRate.exception.ProcessingException
  */
  public double rateCalculateFlat(String priceModel, double valueToRate, long CDRDate)
    throws ProcessingException
  {
    ArrayList<RateMapEntry>  tmpRateModel;
    RatingResult tmpRatingResult;

    // Look up the rate model to use
    tmpRateModel = RC.getPriceModel(priceModel);

    // perform the rating using the selected rate model
    tmpRatingResult = performRateEvaluationFlat(priceModel, tmpRateModel, valueToRate, CDRDate, false);

    // return the rated value
    return tmpRatingResult.RatedValue;
  }

 /**
  * This function does the rating based on the chosen price model and the
  * RUM (Rateable Usage Metric) value. It is a simplified version of the
  * "tiered" model that just does returns the event price.
  *
  * @param  priceModel The price model to use
  * @param CDRDate The date to use for price model version selection
  * @param valueToRate The value to rate for
  * @return the price for the rated record
  * @throws OpenRate.exception.ProcessingException
  */
  public double rateCalculateEvent(String priceModel, long valueToRate, long CDRDate)
    throws ProcessingException
  {
    ArrayList<RateMapEntry>  tmpRateModel;
    RatingResult tmpRatingResult;

    // Look up the rate model to use
    tmpRateModel = RC.getPriceModel(priceModel);

    // perform the rating using the selected rate model
    tmpRatingResult = performRateEvaluationEvent(priceModel, tmpRateModel, valueToRate, CDRDate, false);

    // return the rated value
    return tmpRatingResult.RatedValue;
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
  public double authCalculateTiered(String priceModel, double availableBalance, long CDRDate)
    throws ProcessingException
  {

    if(availableBalance <= 0){
       return 0;
    }

      ArrayList<RateMapEntry>  tmpRateModel;

      double tmpcalculationResult;

    // Look up the rate model to use
    tmpRateModel = RC.getPriceModel(priceModel);

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
  public double authCalculateThreshold(String priceModel, double availableBalance, long CDRDate)
    throws ProcessingException
  {
    if(availableBalance <= 0){
       return 0;
    }

      ArrayList<RateMapEntry>  tmpRateModel;

      double tmpcalculationResult;

    // Look up the rate model to use
    tmpRateModel = RC.getPriceModel(priceModel);

    // perform the calculation using the selected rate model
    tmpcalculationResult = performAuthEvaluationThreshold(priceModel, tmpRateModel, availableBalance, CDRDate);

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
  * This method uses the FLAT rating model.
  *
  * @param priceModel The price model to use
  * @param availableBalance The current balance the user has available to them, positive
  * @param CDRDate The date to rate at
  * @return The number of RUM units that can be purchased for the available balance
  * @throws ProcessingException
  */
  public double authCalculateFlat(String priceModel, double availableBalance, long CDRDate)
    throws ProcessingException
  {
    if(availableBalance <= 0){
       return 0;
    }

      ArrayList<RateMapEntry>  tmpRateModel;
      double tmpcalculationResult;

    // Look up the rate model to use
    tmpRateModel = RC.getPriceModel(priceModel);

    // perform the calculation using the selected rate model
    tmpcalculationResult = performAuthEvaluationFlat(priceModel, tmpRateModel, availableBalance, CDRDate);

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
  * This method uses the EVENT rating model.
  *
  * @param priceModel The price model to use
  * @param availableBalance The current balance the user has available to them, positive
  * @param CDRDate The date to rate at
  * @return The number of RUM units that can be purchased for the available balance
  * @throws ProcessingException
  */
  public double authCalculateEvent(String priceModel, double availableBalance, long CDRDate)
    throws ProcessingException
  {
    if(availableBalance <= 0){
       return 0;
    }
    ArrayList<RateMapEntry>  tmpRateModel;
    double tmpRatingResult;

    // Look up the rate model to use
    tmpRateModel = RC.getPriceModel(priceModel);

    // perform the calculation using the selected rate model
    tmpRatingResult = performAuthEvaluationEvent(priceModel, tmpRateModel, availableBalance, CDRDate);

    return tmpRatingResult;
  }

 /**
  * Performs the rating calculation of the value given at the CDR date
  * using the given rating model. Tiered splits the value to be rated up into
  * segments according to the steps defined and rates each step individually,
  * summing up the charges from all steps.
  *
  * TIERED       Calculation
  * SINGULARITY  Yes
  * BEAT BASED   Yes
  * CHARGE BASE  Yes
  * STEP FROM-TO Yes
  * RATING       beatCount * factor * beat / chargeBase;
  *
  * @param PriceModel The price model name we are using
  * @param tmpRateModel The price model definition
  * @param valueToRate The value to rate
  * @param CDRDate The date to rate at
  * @param BreakDown Produce a charge breakdown or not
  * @return The rating result
  * @throws OpenRate.exception.ProcessingException
  */
  protected RatingResult performRateEvaluationTiered(String PriceModel, ArrayList<RateMapEntry> tmpRateModel, double valueToRate, long CDRDate, boolean BreakDown) throws ProcessingException
  {
    RatingResult tmpRatingResult = new RatingResult();
    int     Index = 0;
    double  ThisTierValue;
    double  ThisTierRUMUsed;
    long    ThisTierBeatCount;
    double  AllTiersValue = 0;
    RateMapEntry tmpEntry;
    double  RUMValueUsed = 0;
    RatingBreakdown tmpBreakdown;

    // check that we have something to work on
    if (tmpRateModel == null)
    {
      throw new ProcessingException("Price Model <" + PriceModel + "> not defined",getSymbolicName());
    }

    // set the default value
    tmpRatingResult.RatedValue = 0;
    tmpRatingResult.RUMUsed = 0;

    // We need to loop through all the tiers until we have finshed
    // consuming all the rateable input
    while (Index < tmpRateModel.size())
    {
      tmpEntry = tmpRateModel.get(Index);

      ThisTierValue = 0;

      // See if this event crosses the lower tier threshold
      if (valueToRate > tmpEntry.getFrom())
      {
        // see if we use all of the tier
        if (valueToRate >= tmpEntry.getTo())
        {
          // Get the validty for this cdr
          tmpEntry = getRateModelEntryForTime(tmpEntry,CDRDate);
          if (tmpEntry == null)
          {
            message = "CDR with <" + CDRDate + "> date not rated by model <" +
                             PriceModel + "> because of missing validity coverage";
            throw new ProcessingException(message,getSymbolicName());
          }

          // Calculate the amount in this tier
          ThisTierRUMUsed = (tmpEntry.getTo() - tmpEntry.getFrom());
          RUMValueUsed = RUMValueUsed + ThisTierRUMUsed;

          // Get the number of beats in this tier
          ThisTierBeatCount = Math.round(ThisTierRUMUsed / tmpEntry.getBeat());

          // Deal with unfinished beats
          if ((ThisTierRUMUsed - ThisTierBeatCount*tmpEntry.getBeat()) > 0)
          {
            ThisTierBeatCount++;
          }

          // Deal with the empty beat
          if (ThisTierBeatCount == 0)
          {
            ThisTierBeatCount = 1;
          }

          // Calculate the value of the tier
          ThisTierValue = (ThisTierBeatCount * tmpEntry.getFactor()) * tmpEntry.getBeat() / tmpEntry.getChargeBase();

          // provide the rating breakdown if it is required
          if (BreakDown)
          {
            // initialise the breakdown if necessary
            if (tmpRatingResult.breakdown == null)
            {
              tmpRatingResult.breakdown = new ArrayList<>();
            }

            // provide the charging breakdown
            tmpBreakdown = new RatingBreakdown();
            tmpBreakdown.beat = tmpEntry.getBeat();
            tmpBreakdown.beatCount = ThisTierBeatCount;
            tmpBreakdown.factor = tmpEntry.getFactor();
            tmpBreakdown.chargeBase = tmpEntry.getChargeBase();
            tmpBreakdown.ratedAmount = ThisTierValue;
            tmpBreakdown.RUMRated = ThisTierRUMUsed;
            tmpBreakdown.stepUsed = Index;
            tmpBreakdown.tierFrom = tmpEntry.getFrom();
            tmpBreakdown.tierTo = tmpEntry.getTo();
            tmpBreakdown.validFrom = tmpEntry.getStartTime();
            tmpBreakdown.validTo = tmpEntry.getEndTime();

            // Store the breakdown
            tmpRatingResult.breakdown.add(tmpBreakdown);
          }
        }
        else
        {
          // Get the validty for this cdr
          tmpEntry = getRateModelEntryForTime(tmpEntry,CDRDate);
          if (tmpEntry == null)
          {
            message = "CDR with <" + CDRDate + "> date not rated by model <" +
                      PriceModel + "> because of missing validity coverage";
            throw new ProcessingException(message,getSymbolicName());
          }

          // Partial tier to do, and then we have finished
          ThisTierRUMUsed = (valueToRate - tmpEntry.getFrom());
          RUMValueUsed = RUMValueUsed + ThisTierRUMUsed;

          ThisTierBeatCount = Math.round(ThisTierRUMUsed / tmpEntry.getBeat());

          // Deal with unfinished beats
          if ((ThisTierRUMUsed - ThisTierBeatCount*tmpEntry.getBeat()) > 0)
          {
            ThisTierBeatCount++;
          }

          // Deal with the empty beat
          if (ThisTierBeatCount == 0)
          {
            ThisTierBeatCount = 1;
          }

          ThisTierValue = (ThisTierBeatCount * tmpEntry.getFactor()) * tmpEntry.getBeat() / tmpEntry.getChargeBase();

          // provide the rating breakdown if it is required
          if (BreakDown)
          {
            // initialise the breakdown if necessary
            if (tmpRatingResult.breakdown == null)
            {
              tmpRatingResult.breakdown = new ArrayList<>();
            }

            // provide the charging breakdown
            tmpBreakdown = new RatingBreakdown();
            tmpBreakdown.beat = tmpEntry.getBeat();
            tmpBreakdown.beatCount = ThisTierBeatCount;
            tmpBreakdown.factor = tmpEntry.getFactor();
            tmpBreakdown.chargeBase = tmpEntry.getChargeBase();
            tmpBreakdown.ratedAmount = ThisTierValue;
            tmpBreakdown.RUMRated = ThisTierRUMUsed;
            tmpBreakdown.stepUsed = Index;
            tmpBreakdown.tierFrom = tmpEntry.getFrom();
            tmpBreakdown.tierTo = tmpEntry.getTo();
            tmpBreakdown.validFrom = tmpEntry.getStartTime();
            tmpBreakdown.validTo = tmpEntry.getEndTime();

            // Store the breakdown
            tmpRatingResult.breakdown.add(tmpBreakdown);
          }
        }
      }

      // Increment the tier counter
      Index++;

      // Accumulate the tier value
      AllTiersValue = AllTiersValue + ThisTierValue;
    }

    // return OK
    tmpRatingResult.RatedValue = AllTiersValue;
    tmpRatingResult.RUMUsed = RUMValueUsed;
    return tmpRatingResult;
  }

 /**
  * Performs the rating calculation of the value given at the CDR date
  * using the given rating model. Threshold calculation locates the step
  * that covers the maximum value to be rated and calculates the whole value
  * to be rated using that step.
  *
  * THRESHOLD    Calculation
  * SINGULARITY  Yes
  * BEAT BASED   Yes
  * CHARGE BASE  Yes
  * STEP FROM-TO Yes
  * RATING       beatCount * factor * beat / chargeBase;
  *
  * @param PriceModel The price model name we are using
  * @param tmpRateModel The price model definition
  * @param valueToRate The value to rate
  * @param CDRDate The date to rate at
  * @param BreakDown Produce a charge breakdown or not
  * @return The rating result
  * @throws OpenRate.exception.ProcessingException
  */
  protected RatingResult performRateEvaluationThreshold(String PriceModel, ArrayList<RateMapEntry> tmpRateModel, double valueToRate, long CDRDate, boolean BreakDown) throws ProcessingException
  {
    int     Index = 0;
    double  ThisTierValue;
    double  ThisTierRUMUsed;
    long    ThisTierBeatCount;
    double  AllTiersValue = 0;
    RateMapEntry tmpEntry;
    double  RUMValueUsed = 0;
    RatingResult tmpRatingResult = new RatingResult();
    RatingBreakdown tmpBreakdown;

    // check that we have something to work on
    if (tmpRateModel == null)
    {
      throw new ProcessingException("Price Model <" + PriceModel + "> not defined",getSymbolicName());
    }

    // We need to loop through all the tiers until we have finshed
    // consuming all the rateable input
    while (Index < tmpRateModel.size())
    {
      tmpEntry = tmpRateModel.get(Index);
      Index++;
      ThisTierValue = 0;

      // See if this event crosses the lower tier threshold
      if (valueToRate > tmpEntry.getFrom())
      {
        // see if we are in this tier
        if (valueToRate < tmpEntry.getTo())
        {
          // Get the validty for this cdr
          tmpEntry = getRateModelEntryForTime(tmpEntry,CDRDate);
          if (tmpEntry == null)
          {
            message = "CDR with <" + CDRDate + "> date not rated by model <" +
                      PriceModel + "> because of missing validity coverage";
            throw new ProcessingException(message,getSymbolicName());
          }

          // Calculate the amount in this tier
          ThisTierRUMUsed = valueToRate;
          RUMValueUsed = RUMValueUsed + ThisTierRUMUsed;

          // Get the number of beats in this tier
          ThisTierBeatCount = Math.round(ThisTierRUMUsed / tmpEntry.getBeat());

          // Deal with unfinished beats
          if ((ThisTierRUMUsed - ThisTierBeatCount*tmpEntry.getBeat()) > 0)
          {
            ThisTierBeatCount++;
          }

          // Deal with the empty beat
          if (ThisTierBeatCount == 0)
          {
            ThisTierBeatCount = 1;
          }

          // Calculate the value of the tier
          ThisTierValue = (ThisTierBeatCount * tmpEntry.getFactor()) * tmpEntry.getBeat() / tmpEntry.getChargeBase();

          // provide the rating breakdown if it is required
          if (BreakDown)
          {
            // initialise the breakdown if necessary
            if (tmpRatingResult.breakdown == null)
            {
              tmpRatingResult.breakdown = new ArrayList<>();
            }

            // provide the charging breakdown
            tmpBreakdown = new RatingBreakdown();
            tmpBreakdown.beat = tmpEntry.getBeat();
            tmpBreakdown.beatCount = ThisTierBeatCount;
            tmpBreakdown.factor = tmpEntry.getFactor();
            tmpBreakdown.chargeBase = tmpEntry.getChargeBase();
            tmpBreakdown.ratedAmount = ThisTierValue;
            tmpBreakdown.RUMRated = ThisTierRUMUsed;
            tmpBreakdown.stepUsed = Index;
            tmpBreakdown.tierFrom = tmpEntry.getFrom();
            tmpBreakdown.tierTo = tmpEntry.getTo();
            tmpBreakdown.validFrom = tmpEntry.getStartTime();
            tmpBreakdown.validTo = tmpEntry.getEndTime();

            // Store the breakdown
            tmpRatingResult.breakdown.add(tmpBreakdown);
          }
        }
        else if (tmpEntry.getFrom() == tmpEntry.getTo())
        {
          // Singularity rate

          // Get the validty for this cdr
          tmpEntry = getRateModelEntryForTime(tmpEntry,CDRDate);
          if (tmpEntry == null)
          {
            message = "CDR with <" + CDRDate + "> date not rated by model <" +
                      PriceModel + "> because of missing validity coverage";
            throw new ProcessingException(message,getSymbolicName());
          }

          // Get the number of beats in this tier
          ThisTierBeatCount = 1;

          // Calculate the value of the tier
          ThisTierValue = (ThisTierBeatCount * tmpEntry.getFactor()) * tmpEntry.getBeat() / tmpEntry.getChargeBase();

          // provide the rating breakdown if it is required
          if (BreakDown)
          {
            // initialise the breakdown if necessary
            if (tmpRatingResult.breakdown == null)
            {
              tmpRatingResult.breakdown = new ArrayList<>();
            }

            // provide the charging breakdown
            tmpBreakdown = new RatingBreakdown();
            tmpBreakdown.beat = tmpEntry.getBeat();
            tmpBreakdown.beatCount = ThisTierBeatCount;
            tmpBreakdown.factor = tmpEntry.getFactor();
            tmpBreakdown.chargeBase = tmpEntry.getChargeBase();
            tmpBreakdown.ratedAmount = ThisTierValue;
            tmpBreakdown.RUMRated = 1;
            tmpBreakdown.stepUsed = Index;
            tmpBreakdown.tierFrom = tmpEntry.getFrom();
            tmpBreakdown.tierTo = tmpEntry.getTo();
            tmpBreakdown.validFrom = tmpEntry.getStartTime();
            tmpBreakdown.validTo = tmpEntry.getEndTime();

            // Store the breakdown
            tmpRatingResult.breakdown.add(tmpBreakdown);
          }
        }
      }

      // Accumulate the tier value
      AllTiersValue = AllTiersValue + ThisTierValue;
    }

    // return OK
    tmpRatingResult.RatedValue = AllTiersValue;
    tmpRatingResult.RUMUsed = RUMValueUsed;
    return tmpRatingResult;
  }

 /**
  * Performs the rating calculation of the value given at the CDR date
  * using the given rating model. Does *NOT* take into account the step FROM
  * and TO values.
  *
  * FLAT         Calculation
  * SINGULARITY  No
  * BEAT BASED   No
  * CHARGE BASE  Yes
  * STEP FROM-TO No
  * RATING       valueToRate * factor / chargeBase
  *
  * @param PriceModel The price model name we are using
  * @param tmpRateModel The price model definition
  * @param valueToRate The value to rate
  * @param CDRDate The date to rate at
  * @param BreakDown Produce a charge breakdown or not
  * @return The rating result
  * @throws OpenRate.exception.ProcessingException
  */
  protected RatingResult performRateEvaluationFlat(String PriceModel, ArrayList<RateMapEntry> tmpRateModel, double valueToRate, long CDRDate, boolean BreakDown) throws ProcessingException
  {
    double  AllTiersValue;
    RateMapEntry tmpEntry;
    double  RUMValueUsed;
    RatingResult tmpRatingResult = new RatingResult();
    RatingBreakdown tmpBreakdown;

    // check that we have something to work on
    if (tmpRateModel == null)
    {
      throw new ProcessingException("Price Model <" + PriceModel + "> not defined",getSymbolicName());
    }

    // Get just the first tier
    tmpEntry = tmpRateModel.get(0);

    // Get the validty for this cdr
    tmpEntry = getRateModelEntryForTime(tmpEntry,CDRDate);
    if (tmpEntry == null)
    {
      message = "CDR with <" + CDRDate + "> date not rated by model <" +
                PriceModel + "> because of missing validity coverage";
      throw new ProcessingException(message,getSymbolicName());
    }

    // Calculate the value of the entry - there should be no others
    AllTiersValue = (valueToRate * tmpEntry.getFactor()) / tmpEntry.getChargeBase();
    RUMValueUsed = valueToRate;

    // provide the rating breakdown if it is required
    if (BreakDown)
    {
      // initialise the breakdown if necessary
      if (tmpRatingResult.breakdown == null)
      {
        tmpRatingResult.breakdown = new ArrayList<>();
      }

      // provide the charging breakdown
      tmpBreakdown = new RatingBreakdown();
      tmpBreakdown.beat = 1;
      tmpBreakdown.beatCount = (long) valueToRate;
      tmpBreakdown.factor = tmpEntry.getFactor();
      tmpBreakdown.chargeBase = tmpEntry.getChargeBase();
      tmpBreakdown.ratedAmount = AllTiersValue;
      tmpBreakdown.RUMRated = RUMValueUsed;
      tmpBreakdown.stepUsed = 1;
      tmpBreakdown.tierFrom = tmpEntry.getFrom();
      tmpBreakdown.tierTo = tmpEntry.getTo();
      tmpBreakdown.validFrom = tmpEntry.getStartTime();
      tmpBreakdown.validTo = tmpEntry.getEndTime();

      // Store the breakdown
      tmpRatingResult.breakdown.add(tmpBreakdown);
    }

    // return OK
    tmpRatingResult.RatedValue = AllTiersValue;
    tmpRatingResult.RUMUsed = RUMValueUsed;
    return tmpRatingResult;
  }

 /**
  * Performs the rating calculation of the value given at the CDR date
  * using the given rating model. It does take into account the step FROM
  * and TO values. Step from and step to values are integers in this case.
  *
  * EVENT        Calculation
  * SINGULARITY  No
  * BEAT BASED   No
  * CHARGE BASE  No
  * STEP FROM-TO Yes
  * RATING       valueToRate * factor
  *
  * @param PriceModel The price model name we are using
  * @param tmpRateModel The price model definition
  * @param valueToRate The value to rate for
  * @param CDRDate The date to rate at
  * @param BreakDown Produce a charge breakdown or not
  * @return The rating result
  * @throws OpenRate.exception.ProcessingException
  */
  protected RatingResult performRateEvaluationEvent(String PriceModel, ArrayList<RateMapEntry> tmpRateModel, long valueToRate, long CDRDate, boolean BreakDown) throws ProcessingException
  {
    RatingResult tmpRatingResult = new RatingResult();
    int     Index = 0;
    double  ThisTierValue;
    double  ThisTierRUMUsed;
    double  AllTiersValue = 0;
    RateMapEntry tmpEntry;
    double  RUMValueUsed = 0;
    RatingBreakdown tmpBreakdown;

    // check that we have something to work on
    if (tmpRateModel == null)
    {
      throw new ProcessingException("Price Model <" + PriceModel + "> not defined",getSymbolicName());
    }

    // set the default value
    tmpRatingResult.RatedValue = 0;
    tmpRatingResult.RUMUsed = 0;

    // We need to loop through all the tiers until we have finshed
    // consuming all the rateable input
    while (Index < tmpRateModel.size())
    {
      tmpEntry = tmpRateModel.get(Index);

      ThisTierValue = 0;

      // See if this event crosses the lower tier threshold
      if (valueToRate > tmpEntry.getFrom())
      {
        // see if we use all of the tier
        if (valueToRate >= tmpEntry.getTo())
        {
          // Get the validty for this cdr
          tmpEntry = getRateModelEntryForTime(tmpEntry,CDRDate);
          if (tmpEntry == null)
          {
            message = "CDR with <" + CDRDate + "> date not rated by model <" +
                             PriceModel + "> because of missing validity coverage";
            throw new ProcessingException(message,getSymbolicName());
          }

          // Calculate the amount in this tier
          ThisTierRUMUsed = (tmpEntry.getTo() - tmpEntry.getFrom());

          // Deal with the case that we have the empty beat
          if (ThisTierRUMUsed == 0)
          {
            ThisTierRUMUsed++;
          }

          RUMValueUsed = RUMValueUsed + ThisTierRUMUsed;

          // Calculate the value of the tier
          ThisTierValue = ThisTierRUMUsed * tmpEntry.getFactor();

          // provide the rating breakdown if it is required
          if (BreakDown)
          {
            // initialise the breakdown if necessary
            if (tmpRatingResult.breakdown == null)
            {
              tmpRatingResult.breakdown = new ArrayList<>();
            }

            // provide the charging breakdown
            tmpBreakdown = new RatingBreakdown();
            tmpBreakdown.beat = tmpEntry.getBeat();
            tmpBreakdown.beatCount = (long) ThisTierRUMUsed;
            tmpBreakdown.factor = tmpEntry.getFactor();
            tmpBreakdown.chargeBase = tmpEntry.getChargeBase();
            tmpBreakdown.ratedAmount = ThisTierValue;
            tmpBreakdown.RUMRated = ThisTierRUMUsed;
            tmpBreakdown.stepUsed = Index;
            tmpBreakdown.tierFrom = tmpEntry.getFrom();
            tmpBreakdown.tierTo = tmpEntry.getTo();
            tmpBreakdown.validFrom = tmpEntry.getStartTime();
            tmpBreakdown.validTo = tmpEntry.getEndTime();

            // Store the breakdown
            tmpRatingResult.breakdown.add(tmpBreakdown);
          }
        }
        else
        {
          // Get the validty for this cdr
          tmpEntry = getRateModelEntryForTime(tmpEntry,CDRDate);
          if (tmpEntry == null)
          {
            message = "CDR with <" + CDRDate + "> date not rated by model <" +
                      PriceModel + "> because of missing validity coverage";
            throw new ProcessingException(message,getSymbolicName());
          }

          // Partial tier to do, and then we have finished
          ThisTierRUMUsed = (valueToRate - tmpEntry.getFrom());

          // Deal with the case that we have the empty beat
          if (ThisTierRUMUsed == 0)
          {
            ThisTierRUMUsed++;
          }

          RUMValueUsed = RUMValueUsed + ThisTierRUMUsed;

          ThisTierValue = ThisTierRUMUsed  * tmpEntry.getFactor();

          // provide the rating breakdown if it is required
          if (BreakDown)
          {
            // initialise the breakdown if necessary
            if (tmpRatingResult.breakdown == null)
            {
              tmpRatingResult.breakdown = new ArrayList<>();
            }

            // provide the charging breakdown
            tmpBreakdown = new RatingBreakdown();
            tmpBreakdown.beat = tmpEntry.getBeat();
            tmpBreakdown.beatCount = (long) ThisTierRUMUsed;
            tmpBreakdown.factor = tmpEntry.getFactor();
            tmpBreakdown.chargeBase = tmpEntry.getChargeBase();
            tmpBreakdown.ratedAmount = ThisTierValue;
            tmpBreakdown.RUMRated = ThisTierRUMUsed;
            tmpBreakdown.stepUsed = Index;
            tmpBreakdown.tierFrom = tmpEntry.getFrom();
            tmpBreakdown.tierTo = tmpEntry.getTo();
            tmpBreakdown.validFrom = tmpEntry.getStartTime();
            tmpBreakdown.validTo = tmpEntry.getEndTime();

            // Store the breakdown
            tmpRatingResult.breakdown.add(tmpBreakdown);
          }
        }
      }

      // Increment the tier counter
      Index++;

      // Accumulate the tier value
      AllTiersValue = AllTiersValue + ThisTierValue;
    }

    // return OK
    tmpRatingResult.RatedValue = AllTiersValue;
    tmpRatingResult.RUMUsed = RUMValueUsed;
    return tmpRatingResult;
  }

 /**
  * Performs the authorisation calculation of the value given at the CDR date.
  * Evaluates all the tiers in the model one at a time, and accumulates the
  * contribution from the tier into the final result.
  *
  * Matches the rating in performRateEvaluationTiered
  *
  * @param PriceModel The price model name we are using
  * @param tmpRateModel The price model definition
  * @param availableBalance The balance available
  * @param CDRDate The date to rate at
  * @return The rating result
  * @throws OpenRate.exception.ProcessingException
  */
  protected double performAuthEvaluationTiered(String PriceModel, ArrayList<RateMapEntry> tmpRateModel, double availableBalance, long CDRDate) throws ProcessingException
  {
    int     Index = 0;
    double  ThisTierValue;
    double  ThisTierRUMUsed;
    long    ThisTierBeatCount;
    double  AllTiersValue = 0;
    RateMapEntry tmpEntry;
    double  RUMValueUsed = 0;
    boolean breakFlag = false;

    // check that we have something to work on
    if (tmpRateModel == null)
    {
      throw new ProcessingException("Price Model <" + PriceModel + "> not defined",getSymbolicName());
    }

    // We need to loop through all the tiers until we have finished
    // consuming all the rateable input
    while (Index < tmpRateModel.size())
    {
      tmpEntry = tmpRateModel.get(Index);
      tmpEntry = getRateModelEntryForTime(tmpEntry,CDRDate);

      if (tmpEntry == null)
      {
        message = "Rate Model entry not valid for CDR with <" + CDRDate + "> date, model <" +
            PriceModel + ">";
        throw new ProcessingException(message,getSymbolicName());
      }

      // Deal with the case that we have a tier without cost
      if (tmpEntry.getFactor() == 0)
      {
        // we just skip over - nothing we can do with 0 priced tiers
        continue;
      }

      // Calculate the amount in this tier
      ThisTierRUMUsed = (tmpEntry.getTo() - tmpEntry.getFrom());

      // Get the number of beats in this tier
      ThisTierBeatCount = Math.round(ThisTierRUMUsed / tmpEntry.getBeat());

      // Deal with unfinished beats
      if ((ThisTierRUMUsed - ThisTierBeatCount*tmpEntry.getBeat()) > 0)
      {
        ThisTierBeatCount++;
      }

      // Deal with the empty beat
      if (ThisTierBeatCount == 0)
      {
        ThisTierBeatCount = 1;
      }

      // Calculate the value of the tier
      ThisTierValue = (ThisTierBeatCount * tmpEntry.getFactor()) * tmpEntry.getBeat() / tmpEntry.getChargeBase();

      if(AllTiersValue + ThisTierValue > availableBalance)
      {
        ThisTierValue = availableBalance - AllTiersValue;
        ThisTierBeatCount = Math.round( (ThisTierValue * tmpEntry.getChargeBase()) / (tmpEntry.getFactor() * tmpEntry.getBeat()));
        ThisTierRUMUsed = tmpEntry.getBeat() * ThisTierBeatCount;
        breakFlag = true;
      }

      // Accumulate the tier value
      AllTiersValue = AllTiersValue + ThisTierValue;
      RUMValueUsed = RUMValueUsed + ThisTierRUMUsed;
      if(breakFlag == true){
          break;
      }

      // Increment the tier counter
      Index++;
    }

    // Set the value to maximum if available balance is a lot higher than all tiered???
//    if(availableBalance > AllTiersValue){
//        RUMValueUsed = Double.MAX_VALUE;
//    }
    return RUMValueUsed;
  }

 /**
  * Performs the authorisation calculation of the value given at the CDR date.
  * We have to perform an evaluation for each of the threshold steps in the
  * model and find the lowest non-zero result. When the authorisation runs out,
  * the same process can happen again with less available balance. Not very
  * beautiful, but functional given the non-linear nature of the model.
  *
  * Matches the rating in performRateEvaluationThreshold
  *
  * @param PriceModel The price model name we are using
  * @param tmpRateModel The price model definition
  * @param availableBalance The balance available
  * @param CDRDate The date to rate at
  * @return The rating result
  * @throws OpenRate.exception.ProcessingException
  */
  protected double performAuthEvaluationThreshold(String PriceModel, ArrayList<RateMapEntry> tmpRateModel, double availableBalance, long CDRDate) throws ProcessingException
  {
    int     Index = 0;
    double  ThisTierRUMUsed;
    long    ThisTierBeatCount;
    RateMapEntry tmpEntry;
    double RUMValueUsed = 0;
    // check that we have something to work on
    if (tmpRateModel == null)
    {
      throw new ProcessingException("Price Model <" + PriceModel + "> not defined",getSymbolicName());
    }

    // We need to loop through all the tiers and evaluate them, then return the
    // shortest.
    while (Index < tmpRateModel.size())
    {
      tmpEntry = tmpRateModel.get(Index);
      Index++;
      tmpEntry = getRateModelEntryForTime(tmpEntry,CDRDate);

      ThisTierBeatCount = Math.round( (availableBalance * tmpEntry.getChargeBase()) / (tmpEntry.getFactor() * tmpEntry.getBeat()));
      ThisTierRUMUsed = tmpEntry.getBeat() * ThisTierBeatCount;

      // is this a better non-zero result
      if( RUMValueUsed == 0){
          RUMValueUsed = ThisTierRUMUsed;
      }else if(RUMValueUsed > 0 && ThisTierRUMUsed > 0
              && RUMValueUsed > ThisTierRUMUsed
              && ThisTierRUMUsed < tmpEntry.getTo()){

              RUMValueUsed = ThisTierRUMUsed;
      }
    }

    return RUMValueUsed;
  }

 /**
  * Performs the authorisation calculation of the value given at the CDR date.
  *
  * Matches the rating in performRateEvaluationFlat
  *
  * @param PriceModel The price model name we are using
  * @param tmpRateModel The price model definition
  * @param availableBalance The balance available
  * @param CDRDate The date to rate at
  * @return The rating result
  * @throws OpenRate.exception.ProcessingException
  */
  protected double performAuthEvaluationFlat(String PriceModel, ArrayList<RateMapEntry> tmpRateModel, double availableBalance, long CDRDate) throws ProcessingException
  {
    RateMapEntry tmpEntry;
    double tmpcalculationResult;

    // check that we have something to work on
    if (tmpRateModel == null)
    {
      throw new ProcessingException("Price Model <" + PriceModel + "> not defined",getSymbolicName());
    }

    // Get just the first tier
    tmpEntry = tmpRateModel.get(0);

    // Get the validty for this cdr
    tmpEntry = getRateModelEntryForTime(tmpEntry,CDRDate);
    if (tmpEntry == null || tmpEntry.getFactor() == 0 || tmpEntry.getChargeBase() == 0)
    {
      message = "Rate Model entry not valid for CDR with <" + CDRDate + "> date, model <" +
                PriceModel + ">, factor <"+tmpEntry.getFactor()+"and charge base <"+tmpEntry.getChargeBase()+">";
      throw new ProcessingException(message,getSymbolicName());
    }

    // Calculate the value of the entry - there should be no others
    tmpcalculationResult = (availableBalance / tmpEntry.getFactor()) * tmpEntry.getChargeBase();

    return tmpcalculationResult;
  }

 /**
  * Performs the authorisation calculation of the value given at the CDR date.
  *
  * Matches the rating in performRateEvaluationEvent
  *
  * @param PriceModel The price model name we are using
  * @param tmpRateModel The price model definition
  * @param availableBalance The balance available
  * @param CDRDate The date to rate at
  * @return The rating result
  * @throws OpenRate.exception.ProcessingException
  */
  protected double performAuthEvaluationEvent(String PriceModel, ArrayList<RateMapEntry> tmpRateModel, double availableBalance, long CDRDate) throws ProcessingException
  {
    double tmpcalculationResult;
    RateMapEntry tmpEntry;

    // check that we have something to work on
    if (tmpRateModel == null)
    {
      throw new ProcessingException("Price Model <" + PriceModel + "> not defined",getSymbolicName());
    }

    // Get just the first tier
    tmpEntry = tmpRateModel.get(0);

    // Get the validity for this cdr
    tmpEntry = getRateModelEntryForTime(tmpEntry,CDRDate);
    if (tmpEntry == null)
    {
      message = "Rate Model entry not valid for CDR with <" + CDRDate + "> date, model <" +
                PriceModel + ">";
      throw new ProcessingException(message,getSymbolicName());
    }

    tmpcalculationResult = availableBalance / tmpEntry.getFactor();

    return tmpcalculationResult;
  }

 /**
  * Runs through the validity periods in a rate map, and returns the one
  * valid for a given date, or null if no match
  *
  * @param tmpEntry The rate map object to search
  * @param CDRDate The long UTC date to search for
  * @return The relevant rate map entry, or null if there was no match at the time
  */
  protected RateMapEntry getRateModelEntryForTime(RateMapEntry tmpEntry, long CDRDate)
  {
    // get to the right validity segment - we know that if the first one has
    // high date, it is likely to be the only one
    if (tmpEntry.getEndTime() < CommonConfig.HIGH_DATE)
    {
      // this is a time based model, need to work with validities
      // check for uncovered start
      if (tmpEntry.getStartTime() > CDRDate)
      {
        return null;
      }

      // get the segment in the middle - we should never get to the end
      // as the last bit is always valid until HIGH DATE
      while (!((tmpEntry.getStartTime() <= CDRDate) & (tmpEntry.getEndTime() > CDRDate)))
      {
        // try to move down the list
        if (tmpEntry.getChild() == null)
        {
          // no more, so we can't rate this
          return null;
        }
        {
          // move down the list
          tmpEntry = tmpEntry.getChild();
        }
      }
    }
    else
    {
      // see if the validity start is OK too
      if (tmpEntry.getStartTime() <= CDRDate)
      {
        // matched
        return tmpEntry;
      }
      else
      {
        return null;
      }
    }

    // return the right bit
    return tmpEntry;
  }
}
