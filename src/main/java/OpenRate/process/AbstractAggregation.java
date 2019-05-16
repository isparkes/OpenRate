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

import OpenRate.cache.AggregationCache;
import OpenRate.cache.ICacheManager;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * This class provides the abstract base for an aggregation plugin, matching an
 * strSymbolicName class. This abstract class takes care of locating the
 * cache class and dealing with the transaction handling.
 *
 * The aggregation processing has become more complex with the arrival of the
 * "Overlaid transaction handling" (multiple transactions that can run in
 * parallel, so that more than one transaction can be processed at once.
 *
 * This means that each of the transactions must exist in it's own context, and
 * should only be accumulated into the overall transaction context when the
 * transaction is committed.
 *
 * Results are written out to the results file at the end of the transaction.
 */
public abstract class AbstractAggregation
  extends AbstractTransactionalPlugIn
{
  // This is the aggregation cache that stores the data that is aggregated
  private ICacheManager CMAggCache = null;

  /**
   * This is the aggregation module
   */
  protected AggregationCache aggCache = null;

  // This is the base name of the output file
  private String AggOutBaseName;

  // This is used to purge transaction results every n transactions
  private int TransactionsSinceWrite = 0;

  // this is how often we write results to the output
  private int writeResultFrequency = 1;

  // This is used to hold the file names
  private class TransControlStructure
  {
    String BaseName;
  }

  // This holds the file names for the files that are in processing at any
  // given moment
  private HashMap <Integer, TransControlStructure> currentFileNames;

  // List of Services that this Client supports
  private final static String SERVICE_WRITE_EVERY_N_TRANS  = "WriteResultFrequency";

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
  * @param ModuleName The name of the module
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String PipelineName, String ModuleName)
            throws InitializationException
  {
    // Variable for holding the cache object name
    String CacheObjectName;
    String helper;
    super.init(PipelineName, ModuleName);

    // ------------------------- Agg Cache ---------------------------------
    // Get the cache object reference
    CacheObjectName = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(PipelineName,
                                                           ModuleName,
                                                          "AggCache",
                                                          "None");
    if (CacheObjectName.equalsIgnoreCase("None"))
    {
      message = "Could not find cache entry for <AggCache>";
      throw new InitializationException(message,getSymbolicName());
    }

    CMAggCache = CacheFactory.getGlobalManager(CacheObjectName);

    if (CMAggCache == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Get the reference to the Auth List
    aggCache = (AggregationCache)CMAggCache.get(CacheObjectName);

    // initialise the file name object
    currentFileNames = new HashMap <>(10);

    // see if we want to initialise the write result frequency
    helper = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(PipelineName,
                                                           ModuleName,
                                                          SERVICE_WRITE_EVERY_N_TRANS,
                                                          "None");

    if (helper.equals("None") == false)
    {
      int tmpFreq = 0;

      try
      {
        tmpFreq = Integer.parseInt(helper);
      }
      catch (NumberFormatException nfe)
      {
        message = "Invalid value <" + helper + "> for <" + SERVICE_WRITE_EVERY_N_TRANS + ">";
        throw new InitializationException(message,getSymbolicName());
      }

      // negative values not allowed
      if (tmpFreq < 1)
      {
        message = "Negative/zero value <" + helper + "> not supported for <" + SERVICE_WRITE_EVERY_N_TRANS + ">";
        throw new InitializationException(message,getSymbolicName());
      }
      else
      {
        writeResultFrequency = tmpFreq;
      }
    }
  }

 /**
  * Mark the transaction as started when we get the start of stream header
  *
  * @param r The header record
  * @return The unmodified header record
  */
  @Override
  public IRecord procHeader(IRecord r)
  {
    HeaderRecord tmpHeader;
    TransControlStructure tmpTransControl;

    // perform the super processing that starts the transaction
    super.procHeader(r);

    // Add the name to the list
    tmpHeader = (HeaderRecord)r;
    tmpTransControl = new TransControlStructure();
    tmpTransControl.BaseName = tmpHeader.getStreamName();

    currentFileNames.put(getTransactionNumber(), tmpTransControl);

    return r;
  }

  // -----------------------------------------------------------------------------
  // ------------------ Start of transaction layer functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * See if we can start the transaction
  *
  * @param transactionNumber The number of the transaction
  * @return 0 if the transaction can start
  */
  @Override
  public int startTransaction(int transactionNumber)
  {
    return 0;
  }

 /**
  * See if the transaction was flushed correctly
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
  * Do the work that is needed to commit the transaction
  *
  * @param transactionNumber The number of the transaction
  */
  @Override
  public void commitTransaction(int transactionNumber)
  {
    // Commit the results
    aggCache.commitTransaction(transactionNumber);

    TransactionsSinceWrite++;

    // We write the results out if we have been configured to (0 means never do so)
    // and it is time to do so
    if (( writeResultFrequency > 0) & (TransactionsSinceWrite >= writeResultFrequency))
    {
      // Get the name of the results out file
      AggOutBaseName = getAggregationFileBaseName(transactionNumber);

      // Write out the results and purge them
      aggCache.writeResults(AggOutBaseName);

      // Reset the counter
      TransactionsSinceWrite = 0;
    }

    // Clean up the file list
    currentFileNames.remove(transactionNumber);
  }

 /**
  * Do the work that is needed to roll the transaction back
  *
  * @param transactionNumber The number of the transaction
  */
  @Override
  public void rollbackTransaction(int transactionNumber)
  {
    // Discard the transaction the results
    aggCache.rollbackTransaction(transactionNumber);

    // Clean up the file list
    currentFileNames.remove(transactionNumber);
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
    // Clean up the file names array
    currentFileNames.remove(transactionNumber);
  }

// -----------------------------------------------------------------------------
// --------------- Start of custom implementation functions --------------------
// -----------------------------------------------------------------------------

 /**
  * This is used to calculate the stream base name for the aggregation file.
  * Override this if you want a different name to the standard one.
  *
  * @param transactionNumber The transaction to get the file base name for
  * @return The unmodified header record
  */
  public String getAggregationFileBaseName(int transactionNumber)
  {
    return "Agg" + Integer.toString(transactionNumber);
  }

 /**
  * This function performs the aggregation according to the configuration
  *
  * @param fieldList The list of fields to work on
  * @param keysToAggregate The keys to aggregate
  * @throws ProcessingException
  */
  public void Aggregate(String[] fieldList, ArrayList<String> keysToAggregate)
    throws ProcessingException
  {
    aggCache.aggregate(fieldList, keysToAggregate, getTransactionNumber());
  }

 /**
  * Utility function to get the base name of the stream.
  *
  * @param transactionNumber the transaction number to get for
  * @return The base name for the transaction
  */
  public String getBaseName(int transactionNumber)
  {
    return currentFileNames.get(transactionNumber).BaseName;
  }

// -----------------------------------------------------------------------------
// ------------- Start of inherited IEventInterface functions ------------------
// -----------------------------------------------------------------------------

 /**
  * registerClientManager registers this class as a client of the ECI listener
  * and publishes the commands that the plug in understands. The listener is
  * responsible for delivering only these commands to the plug in.
  *
  */
  @Override
  public void registerClientManager() throws InitializationException
  {
    //Register this Client
    ClientManager.getClientManager().registerClient(getPipeName(),getSymbolicName(), this);

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_WRITE_EVERY_N_TRANS, ClientManager.PARAM_DYNAMIC);
  }

 /**
  * processControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world, for
  * example turning the dumping on and off.
  *
  * @param command The command that we are to work on
  * @param init True if the pipeline is currently being constructed
  * @param parameter The parameter value for the command
  * @return The result message of the operation
  */
  @Override
  public String processControlEvent(String command, boolean init, String parameter)
  {
    int ResultCode = -1;

    // Set the batch size
    if (command.equalsIgnoreCase(SERVICE_WRITE_EVERY_N_TRANS))
    {
      if (parameter.equals(""))
      {
        return Integer.toString(writeResultFrequency);
      }
      else
      {
        try
        {
          writeResultFrequency = Integer.parseInt(parameter);
        }
        catch (NumberFormatException nfe)
        {
          getPipeLog().error("Invalid number for result write frequency. Passed value = <" + parameter + ">");
        }
        ResultCode = 0;
      }
    }


    if (ResultCode == 0)
    {
      getPipeLog().debug(LogUtil.LogECIPipeCommand(getSymbolicName(), getPipeName(), command, parameter));

      return "OK";
    }
    else
    {
      // This is not our event, pass it up the stack
      return super.processControlEvent(command, init, parameter);
    }
  }
}
