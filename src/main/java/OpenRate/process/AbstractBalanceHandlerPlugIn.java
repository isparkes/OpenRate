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

import OpenRate.cache.BalanceCache;
import OpenRate.cache.ICacheManager;
import OpenRate.exception.InitializationException;
import OpenRate.lang.BalanceGroup;
import OpenRate.lang.Counter;
import OpenRate.lang.DiscountInformation;
import OpenRate.record.BalanceImpact;
import OpenRate.record.IRatingRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;

/**
 * Now that we have the prioritised list of products and promotions, we can
 * work out the consuming of the balances that there might be, before we pass
 * into rating the values of what is left after consumption. This will decrement
 * balances, passing the results on for rating.
 */
public abstract class AbstractBalanceHandlerPlugIn extends AbstractTransactionalPlugIn
{
  // get the Cache manager for the balance cache
  private ICacheManager BG;

 /**
  * The balance cache object
  */
  protected BalanceCache BC;

 /**
  * The discount flag tells us what the discounting module did. The value
  * DISCOUNT_FLAG_NO_DISCOUNT tells us that the discount was not applied.
  */
  public final static int DISCOUNT_FLAG_NO_DISCOUNT = 0;

 /**
  * The discount flag tells us what the discounting module did. The value
  * DISCOUNT_FLAG_FULLY_DISCOUNTED tells us that the discount was applied and
  * the discount was not exhausted. The event was therefore fully discounted.
  */
  public final static int DISCOUNT_FLAG_FULLY_DISCOUNTED = 1;

 /**
  * The discount flag tells us what the discounting module did. The value
  * DISCOUNT_FLAG_PARTIALLY_DISCOUNTED tells us that the discount was applied
  * but the discount was exhausted. The event was therefore only discounted for
  * some of the value of the event.
  */
  public final static int DISCOUNT_FLAG_PARTIALLY_DISCOUNTED = 2;

 /**
  * The discount flag tells us what the discounting module did. The value
  * DISCOUNT_FLAG_REFUNDED tells us that the discount was refunded.
  */
  public final static int DISCOUNT_FLAG_REFUNDED = 3;

 /**
  * The discount flag tells us what the discounting module did. The value
  * DISCOUNT_FLAG_AGGREGATED tells us that the aggregation was applied onto the
  * discount.
  */
  public final static int DISCOUNT_FLAG_AGGREGATED = 4;

  // -----------------------------------------------------------------------------
  // ------------------ Start of initialisation functions ------------------------
  // -----------------------------------------------------------------------------

 /**
  * Initialise the module. Called during pipeline creation to initialise:
  *  - Configuration properties that are defined in the properties file.
  *  - The references to any cache objects that are used in the processing
  *  - The symbolic name of the module
  *
  * @param pipelineName The name of the pipeline this module is in
  * @param moduleName The name of this module in the pipeline
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String pipelineName, String moduleName)
    throws InitializationException
  {
    String CacheObjectName;

    // Register ourself with the client manager
    setSymbolicName(moduleName);

    // do the inherited initialisation
    super.init(pipelineName,moduleName);

    // Get the cache object reference
    CacheObjectName = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(pipelineName,
                                                           moduleName,
                                                           "DataCache",
                                                           "None");
    
    if (CacheObjectName.equals("None"))
    {
      message = "Not able to find cache property entry for module <"+moduleName+"> in pipeline <"+pipelineName+">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Load up the customer information held in the Cached Object
    BG = CacheFactory.getGlobalManager(CacheObjectName);

    if (BG == null)
    {
      message = "Could not find cache entry for cache <" + CacheObjectName + "> in module <"+moduleName+"> in pipeline <"+pipelineName+">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Load up the mapping arrays
    BC = (BalanceCache)BG.get(CacheObjectName);

    if (BC == null)
    {
      getPipeLog().fatal("Could not find cache entry for <" + CacheObjectName + ">");
      throw new InitializationException("Could not find cache entry for <" +
                                        CacheObjectName + ">",getSymbolicName());
    }
  }

  // -----------------------------------------------------------------------------
  // --------------- Start of transactional layer functions ----------------------
  // -----------------------------------------------------------------------------

 /**
  * Called when the underlying transaction is commanded to start.
  * This should return 0 if everything was OK, otherwise -1.
  *
  * @param transactionNumber
  */
  @Override
  public int startTransaction(int transactionNumber)
  {
    return 0;
  }

 /**
  * Called when the underlying transaction is commanded to FLUSH, that means to
  * close down all processing objects and go into a quiescent state.
  * This should return 0 if everything was OK, otherwise -1.
  *
  * @param transactionNumber The number of the transaction
  * @return 0 if the transaction was flushed OK
  */
  @Override
  public int flushTransaction(int transactionNumber)
  {
    return 0;
  }

 /**
  * Called when the underlying transaction is commanded to commit that means to
  * fix any data and finish.
  *
  * @param transactionNumber The number of the transaction
  */
  @Override
  public void commitTransaction(int transactionNumber)
  {
    // NOP
  }

 /**
  * Called when the underlying transaction is commanded to roll back, that means
  * to undo any data and finish.
  *
  * @param transactionNumber The number of the transaction
  */
  @Override
  public void rollbackTransaction(int transactionNumber)
  {
    // NOP
  }

 /**
  * Close Transaction is the trigger to clean up transaction related information
  * such as variables, status etc.
  *
  * @param transactionNumber The transaction we are working on
  */
  @Override
  public void closeTransaction(int transactionNumber)
  {
    // NOP
  }

// -----------------------------------------------------------------------------
// ----------------- Start of low level exposed functions ----------------------
// -----------------------------------------------------------------------------

 /**
  * Get a balance group from the cache
  *
  * @param balanceGroupId The balance group to recover
  * @return The balance group, or null if not found
  */
  public BalanceGroup getBalanceGroup(long balanceGroupId)
  {
    return BC.getBalanceGroup(balanceGroupId);
  }

 /**
  * Add a new balance group to the cache
  *
  * @param balanceGroupId The Id of the group to add
  * @return The newly created balance group
  */
  public BalanceGroup addBalanceGroup(long balanceGroupId)
  {
    return BC.addBalanceGroup(balanceGroupId);
  }

 /**
  * Check if a balance exists at the given date
  *
  * @param balanceGroupId The id of the balance group
  * @param counterId The id of the counter
  * @param utcEventDate The date to check for
  * @return True if the balance exists, otherwise false
  */
  public Counter checkCounterExists(long balanceGroupId, int counterId, long utcEventDate)
  {
    return BC.checkCounterExists(balanceGroupId, counterId, utcEventDate);
  }

 /**
  * Add a value into the BalanceCache. Does not check for current existence.
  * Intended for use by user applications, as it lets the counter group manage
  * the record id.
  *
  * @param balanceGroupId The id of the balance group
  * @param counterId The ID of the counter in the balance group
  * @param validFrom The start of the validity period for the counter period
  * @param validTo The end of the validity period for the counter period
  * @param currentBal The current balance to assign to the counter period
  * @return The created counter
  */
  public Counter addCounter(long balanceGroupId, int counterId, long validFrom, long validTo, double currentBal)
  {
    return BC.addCounter(balanceGroupId, counterId, validFrom, validTo, currentBal);
  }

 /**
  * Gets a counter from a balance group by counter id and UTC date
  *
  * @param balanceGroup The balance group we are dealing with
  * @param counterId The counter id to retrieve for
  * @param utcEventDate The date to retrieve for
  * @return The counter or null
  */
  public Counter getCounter(long balanceGroup, int counterId, long utcEventDate)
  {
    return BC.getCounter(balanceGroup, counterId, utcEventDate);
  }

 /**
  * Gets a counter balance from a balance group by counter id and UTC date
  *
  * @param balanceGroup The balance group to retrieve for
  * @param counterId The counter id to retrieve for
  * @param utcEventDate The date to retrieve for
  * @param initialValue The initial value of the counter in the case we create it
  * @return The counter or null
  */
  public double getCounterBalance(long balanceGroup, int counterId, long utcEventDate, double initialValue)
  {
    Counter tmpCounterReq;
    tmpCounterReq = BC.getCounter(balanceGroup, counterId, utcEventDate);

    if (tmpCounterReq == null)
    {
      return initialValue;
    }
    else
    {
      return tmpCounterReq.CurrentBalance;
    }
  }

// -----------------------------------------------------------------------------
// ----------------- Start of high level exposed functions ---------------------
// -----------------------------------------------------------------------------

 /**
  * Consumes the given RUM value from a IRatingRecord record, creating a balance
  * if this is not found. The balance impacts are written directly back into
  * the record, and a discounting summary is passed back to the caller.
  *
  * This method assumes that the counter has an initial value, which is
  * progressively consumed towards 0.
  *
  * The RUM value is reduced by the amount that could be allocated to the
  * balance. (i.e. It is consumed).
  *
  * This method will consume up until exhaustion of the counter value, splitting
  * the input value if we are crossing a threshold.
  *
  * The discounting summary tells the user:
  *   - If the record was discounted
  *   - If a balance impact was created
  *   - The ID and Record ID of the counter impacted
  *   - The value consumed
  *   - The value of the counter after the impact
  *
  * In addition the following fields from the IRatingRecord are used:
  *   - UTCEventDate
  *
  * Some parameters are only used if the counter bucket is created. These are
  * marked with (*).
  *
  * @param currentRecord The record to be discounted, inherited from IRatingRecord
  * @param discountName The name of the discount
  * @param balanceGroupId The ID of the balance group
  * @param rumToUse The RUM to consume
  * @param counterId The ID of the counter to impact
  * @param initialBalance The initial value of the counter (*)
  * @param utcBalanceStartValidity The start of bucket validity (*)
  * @param UTCBalanceEndValidity The end of bucket validity (*)
  * @return The DiscountInformation summary object
  */
  public DiscountInformation discountConsumeRUM(IRatingRecord currentRecord, String discountName, long balanceGroupId, String rumToUse, int counterId, double initialBalance, long utcBalanceStartValidity, long UTCBalanceEndValidity)
  {
    BalanceImpact tmpBalImpact;
    double tmpRUMValue;
    double tmpDiscount;

    DiscountInformation tmpReturnInfo = new DiscountInformation();

    tmpRUMValue = currentRecord.getRUMValue(rumToUse);
    Counter tmpCounter = checkCounterExists(balanceGroupId, counterId, currentRecord.getUTCEventDate());

    if (tmpCounter == null)
    {
      tmpCounter = addCounter(balanceGroupId,counterId,utcBalanceStartValidity,UTCBalanceEndValidity,initialBalance);

      // Add the balance impact
      tmpBalImpact = new BalanceImpact();
      tmpBalImpact.type = "D";
      tmpBalImpact.balanceGroup = balanceGroupId;
      tmpBalImpact.cpiName = discountName;
      tmpBalImpact.ruleName = "CREATION";
      tmpBalImpact.rumUsed = rumToUse;
      tmpBalImpact.counterID = counterId;
      tmpBalImpact.recID = tmpCounter.RecId;
      tmpBalImpact.rumValueAfter = 0.0;
      tmpBalImpact.rumValueUsed = 0;
      tmpBalImpact.balanceAfter = initialBalance;
      tmpBalImpact.balanceDelta = initialBalance;
      tmpBalImpact.startDate = utcBalanceStartValidity;
      tmpBalImpact.endDate = UTCBalanceEndValidity;

      if (tmpBalImpact.balanceDelta != 0)
      {
        currentRecord.addBalanceImpact(tmpBalImpact);

        tmpReturnInfo.setBalanceCreated(true);
      }
    }

    // see if we have used up all of the counter
    if (tmpRUMValue > tmpCounter.CurrentBalance)
    {
      if (tmpCounter.CurrentBalance <= 0)
      {
        // we have used up the counter, leave cost alone
      }
      else
      {
        // we are crossing a threshold
        tmpDiscount = tmpCounter.CurrentBalance;
        currentRecord.updateRUMValue(rumToUse,-tmpCounter.CurrentBalance);
        tmpCounter.CurrentBalance = 0;

        // Add the balance impact
        tmpBalImpact = new BalanceImpact();
        tmpBalImpact.type = "D";
        tmpBalImpact.balanceGroup = balanceGroupId;
        tmpBalImpact.cpiName = discountName;
        tmpBalImpact.ruleName = "Consume" + rumToUse;
        tmpBalImpact.rumUsed = rumToUse;
        tmpBalImpact.counterID = counterId;
        tmpBalImpact.recID = tmpCounter.RecId;
        tmpBalImpact.rumValueAfter = currentRecord.getRUMValue(rumToUse);
        tmpBalImpact.rumValueUsed = tmpDiscount;
        tmpBalImpact.balanceAfter = 0;
        tmpBalImpact.balanceDelta = -tmpDiscount;
        tmpBalImpact.startDate = tmpCounter.validFrom;
        tmpBalImpact.endDate = tmpCounter.validTo;

        if (tmpBalImpact.balanceDelta != 0)
        {
          currentRecord.addBalanceImpact(tmpBalImpact);

          // Prepare the return value
          tmpReturnInfo.setDiscountApplied(true);
          tmpReturnInfo.setCounterId(counterId);
          tmpReturnInfo.setRecId(tmpCounter.RecId);
          tmpReturnInfo.setDiscountedValue(tmpDiscount);
          tmpReturnInfo.setNewBalanceValue(0);            // was implicitly 0, now explicit

          // Set the discount flag to "threshold crossing"
          tmpReturnInfo.setDiscountFlag(DISCOUNT_FLAG_PARTIALLY_DISCOUNTED);
        }
      }
    }
    else
    {
      if (tmpCounter.CurrentBalance <= 0)
      {
        // we have used up the counter, leave Volume alone
      }
      else
      {
        // we are just decrementing the counter, using all of the impact
        tmpCounter.CurrentBalance -= tmpRUMValue;
        tmpDiscount = tmpRUMValue;
        currentRecord.updateRUMValue(rumToUse,-currentRecord.getRUMValue(rumToUse));
        tmpReturnInfo.setDiscountApplied(true);

        // Add the balance impact
        tmpBalImpact = new BalanceImpact();
        tmpBalImpact.type = "D";
        tmpBalImpact.balanceGroup = balanceGroupId;
        tmpBalImpact.cpiName = discountName;
        tmpBalImpact.ruleName = "Consume" + rumToUse;
        tmpBalImpact.rumUsed = rumToUse;
        tmpBalImpact.counterID = counterId;
        tmpBalImpact.recID = tmpCounter.RecId;
        tmpBalImpact.rumValueAfter = 0.0;
        tmpBalImpact.rumValueUsed = tmpDiscount;
        tmpBalImpact.balanceAfter = tmpCounter.CurrentBalance;
        tmpBalImpact.balanceDelta = -tmpDiscount;
        tmpBalImpact.startDate = tmpCounter.validFrom;
        tmpBalImpact.endDate = tmpCounter.validTo;

        if (tmpBalImpact.balanceDelta != 0)
        {
          currentRecord.addBalanceImpact(tmpBalImpact);

          // Prepare the return value
          tmpReturnInfo.setDiscountApplied(true);
          tmpReturnInfo.setCounterId(counterId);
          tmpReturnInfo.setRecId(tmpCounter.RecId);
          tmpReturnInfo.setDiscountedValue(tmpDiscount);
          tmpReturnInfo.setNewBalanceValue(tmpCounter.CurrentBalance);

          // Set the discount flag to "fully discounted"
          tmpReturnInfo.setDiscountFlag(DISCOUNT_FLAG_FULLY_DISCOUNTED);
        }
      }
    }

    return tmpReturnInfo;
  }

 /**
  * Refunds a previous consumed RUM value from a IRatingRecord record. The
  * balance impacts are written directly back into the record, and a discounting
  * summary is passed back to the caller.
  *
  * The RUM value is increased by the amount that could be allocated to the
  * balance. (i.e. Consumption is refunded). We don't change the RUM value in
  * this case, it would have no meaning.
  *
  * The discounting summary tells the user:
  *   - If the record was discounted
  *   - If a balance impact was created
  *   - The ID and Record ID of the counter impacted
  *   - The value consumed
  *   - The value of the counter after the impact
  *
  * In addition the following fields from the IRatingRecord are used:
  *   - UTCEventDate
  *
  * @param currentRecord The record to be discounted, inherited fro IRatingRecord
  * @param discountName The name of the discount
  * @param balanceGroupId The ID of the balance group
  * @param rumToUse The RUM to consume
  * @param counterId The ID of the counter to impact
  * @param initialBalance The initial balance the counter had, serves as a maximum limit in refunds
  * @return The DiscountInformation summary object
  */
  public DiscountInformation refundConsumeRUM(IRatingRecord currentRecord, String discountName, long balanceGroupId, String rumToUse, int counterId, double initialBalance)
  {
    BalanceImpact tmpBalImpact;
    double tmpRUMValue;
    double tmpDiscount;

    DiscountInformation tmpReturnInfo = new DiscountInformation();

    tmpRUMValue = currentRecord.getRUMValue(rumToUse);
    Counter tmpCounter = checkCounterExists(balanceGroupId, counterId, currentRecord.getUTCEventDate());

    if (tmpCounter == null)
    {
      // can't refund onto a non-existent counter
      return null;
    }

    // we give the value back
    if ((tmpCounter.CurrentBalance + tmpRUMValue) > initialBalance)
    {
      // we can't go over the initial value, so limit what we refund
      tmpRUMValue = initialBalance - tmpCounter.CurrentBalance;
    }

    tmpCounter.CurrentBalance += tmpRUMValue;
    tmpDiscount = tmpRUMValue;

    // Add the balance impact
    tmpBalImpact = new BalanceImpact();
    tmpBalImpact.type = "D";
    tmpBalImpact.balanceGroup = balanceGroupId;
    tmpBalImpact.cpiName = discountName;
    tmpBalImpact.ruleName = "Refund" + rumToUse;
    tmpBalImpact.rumUsed = rumToUse;
    tmpBalImpact.counterID = counterId;
    tmpBalImpact.recID = tmpCounter.RecId;
    tmpBalImpact.rumValueAfter = tmpCounter.CurrentBalance;
    tmpBalImpact.rumValueUsed = currentRecord.getRUMValue(rumToUse);
    tmpBalImpact.balanceAfter = tmpCounter.CurrentBalance;
    tmpBalImpact.balanceDelta = tmpDiscount;
    tmpBalImpact.startDate = tmpCounter.validFrom;
    tmpBalImpact.endDate = tmpCounter.validTo;

    if (tmpBalImpact.balanceDelta != 0)
    {
      tmpReturnInfo.setDiscountApplied(true);

      currentRecord.addBalanceImpact(tmpBalImpact);

      // Prepare the return value
      tmpReturnInfo.setDiscountApplied(true);
      tmpReturnInfo.setCounterId(counterId);
      tmpReturnInfo.setRecId(tmpCounter.RecId);
      tmpReturnInfo.setDiscountedValue(tmpDiscount);
      tmpReturnInfo.setNewBalanceValue(tmpCounter.CurrentBalance);

      // Set the discount flag to "refund"
      tmpReturnInfo.setDiscountFlag(DISCOUNT_FLAG_REFUNDED);
    }

    return tmpReturnInfo;
  }

 /**
  * Aggregates the given RUM value from a IRatingRecord record, creating a balance
  * if this is not found. The balance impacts are written directly back into
  * the record, and a discounting summary is passed back to the caller.
  *
  * This method assumes that the counter has 0 as an initial value, which is
  * progressively incremented.
  *
  * The RUM value is left untouched (it is not consumed).
  *
  * The discounting summary tells the user:
  *   - If the record was discounted
  *   - If a balance impact was created
  *   - The ID and Record ID of the counter impacted
  *   - The value aggregated
  *   - The value of the counter after the impact
  *
  * In addition the following fields from the IRatingRecord are used:
  *   - UTCEventDate
  *
  * Some parameters are only used if the counter bucket is created. These are
  * marked with (*).
  *
  * @param currentRecord The record to be discounted, inherited fro IRatingRecord
  * @param discountName The name of the discount
  * @param balanceGroupId The ID of the balance group
  * @param rumToUse The RUM to consume
  * @param counterId The ID of the counter to impact
  * @param initialBalance The initial value of the counter (*)
  * @param utcBalanceStartValidity The start of bucket validity (*)
  * @param UTCBalanceEndValidity The end of bucket validity (*)
  * @return The DiscountInformation summary object
  */
  public DiscountInformation discountAggregateRUM(IRatingRecord currentRecord, String discountName, long balanceGroupId, String rumToUse, int counterId, double initialBalance, long utcBalanceStartValidity, long UTCBalanceEndValidity)
  {
    BalanceImpact tmpBalImpact;
    double tmpRUMValue;
    double tmpDiscount;

    DiscountInformation tmpReturnInfo = new DiscountInformation();

    tmpRUMValue = currentRecord.getRUMValue(rumToUse);
    Counter tmpCounter = checkCounterExists(balanceGroupId, counterId, currentRecord.getUTCEventDate());

    if (tmpCounter == null)
    {
      tmpCounter = addCounter(balanceGroupId,counterId,utcBalanceStartValidity,UTCBalanceEndValidity,initialBalance);

      // Add the balance impact
      tmpBalImpact = new BalanceImpact();
      tmpBalImpact.type = "D";
      tmpBalImpact.balanceGroup = balanceGroupId;
      tmpBalImpact.cpiName = discountName;
      tmpBalImpact.ruleName = "CREATION";
      tmpBalImpact.rumUsed = rumToUse;
      tmpBalImpact.counterID = counterId;
      tmpBalImpact.recID = tmpCounter.RecId;
      tmpBalImpact.rumValueAfter = 0.0;
      tmpBalImpact.rumValueUsed = 0;
      tmpBalImpact.balanceAfter = initialBalance;
      tmpBalImpact.balanceDelta = initialBalance;
      tmpBalImpact.startDate = utcBalanceStartValidity;
      tmpBalImpact.endDate = UTCBalanceEndValidity;

      currentRecord.addBalanceImpact(tmpBalImpact);

      tmpReturnInfo.setBalanceCreated(true);
    }

    // now that we are sure we have a balance, update it
    tmpCounter.CurrentBalance += tmpRUMValue;
    tmpDiscount = tmpRUMValue;
    tmpReturnInfo.setDiscountApplied(true);

    // Add the balance impact
    tmpBalImpact = new BalanceImpact();
    tmpBalImpact.type = "D";
    tmpBalImpact.balanceGroup = balanceGroupId;
    tmpBalImpact.cpiName = discountName;
    tmpBalImpact.ruleName = "Aggregate" + rumToUse;
    tmpBalImpact.rumUsed = rumToUse;
    tmpBalImpact.counterID = counterId;
    tmpBalImpact.recID = tmpCounter.RecId;
    tmpBalImpact.rumValueAfter = tmpRUMValue;
    tmpBalImpact.rumValueUsed = tmpRUMValue;
    tmpBalImpact.balanceAfter = tmpCounter.CurrentBalance;
    tmpBalImpact.balanceDelta = tmpDiscount;
    tmpBalImpact.startDate = tmpCounter.validFrom;
    tmpBalImpact.endDate = tmpCounter.validTo;

    if (tmpBalImpact.balanceDelta != 0)
    {
      currentRecord.addBalanceImpact(tmpBalImpact);

      // Prepare the return value
      tmpReturnInfo.setDiscountApplied(true);
      tmpReturnInfo.setCounterId(counterId);
      tmpReturnInfo.setRecId(tmpCounter.RecId);
      tmpReturnInfo.setDiscountedValue(tmpDiscount);
      tmpReturnInfo.setNewBalanceValue(tmpCounter.CurrentBalance);

      // Set the discount flag to "aggregate"
      tmpReturnInfo.setDiscountFlag(DISCOUNT_FLAG_AGGREGATED);
    }

    return tmpReturnInfo;
  }
}
