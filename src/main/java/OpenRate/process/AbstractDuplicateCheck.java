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

import OpenRate.cache.DuplicateCheckCache;
import OpenRate.cache.ICacheManager;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * This class provides the abstract base for an duplicate check
 */
public abstract class AbstractDuplicateCheck
  extends AbstractTransactionalPlugIn
{
  // this is the key we are looking for in the configuration of the module
  private static final String CACHE_KEY = "DataCache";

  // This is the cache that stores the duplicate check data
  private ICacheManager CMDupCache = null;

  /**
   * This is the duplicate check cache module
   */
  private DuplicateCheckCache DupCache = null;

  // calendar object
  private GregorianCalendar newCal;

  // Shows whether the check is active or not
  private boolean Active = true;

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
    // Variable for holding the cache object name
    String CacheObjectName;

    // perform parent initialisation
    super.init(PipelineName, ModuleName);

    // ------------------------- Dup Cache ---------------------------------
    // Get the cache object reference
    CacheObjectName = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(PipelineName,
                                                           ModuleName,
                                                          CACHE_KEY,
                                                          "None");
    if (CacheObjectName.equalsIgnoreCase("None"))
    {
      message = "Could not find cache entry for <" + CACHE_KEY + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    CMDupCache = CacheFactory.getGlobalManager(CacheObjectName);

    if (CMDupCache == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Get the reference to the Auth List
    DupCache = (DuplicateCheckCache)CMDupCache.get(CacheObjectName);

    // Init the calendar object
    newCal = new GregorianCalendar();
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
    // perform the super processing that starts the transaction
    super.procHeader(r);

    return r;
  }

  @Override
  public IRecord procTrailer(IRecord r)
  {
    // perform the super processing that closes the transaction
    super.procTrailer(r);

    return r;
  }

  // -----------------------------------------------------------------------------
  // -------------------------- Start of custom functions ------------------------
  // -----------------------------------------------------------------------------

  /**
   * Check if the record is a duplicate
   *
   * @param CDRDate The date of the CDR
   * @param IDData The Call Reference ID
   * @return false if the call is not a duplicate
   * @throws ProcessingException
   */
  public boolean CheckDuplicate(Date CDRDate, String IDData) throws ProcessingException
  {
    long UTCDate;

    if (Active)
    {
      // Get the UTC time of the record
      newCal.setTime(CDRDate);
      UTCDate = newCal.getTimeInMillis()/1000;

      return DupCache.DuplicateCheck(IDData,UTCDate,getTransactionNumber());
    }
    else
    {
      return false;
    }
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
    DupCache.CreateTransaction(transactionNumber);
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
    // Store the transaction results
    DupCache.CommitTransaction(transactionNumber);
  }

 /**
  * Do the work that is needed to roll the transaction back
  *
  * @param transactionNumber The number of the transaction
  */
  @Override
  public void rollbackTransaction(int transactionNumber)
  {
    // Discard the transaction results
    DupCache.RollbackTransaction(transactionNumber);
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
    // Nothing needed
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
    //ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_WRITE_EVERY_N_TRANS, ClientManager.PARAM_DYNAMIC);
  }

 /**
  * processControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world, for
  * example turning the dumping on and off.
  *
  * @param Command The command that we are to work on
  * @param Init True if the pipeline is currently being constructed
  * @param Parameter The parameter value for the command
  * @return The result message of the operation
  */
  @Override
  public String processControlEvent(String Command, boolean Init, String Parameter)
  {
    int ResultCode = -1;

    // Set the batch size
    /*if (Command.equalsIgnoreCase(SERVICE_WRITE_EVERY_N_TRANS))
    {
      if (Parameter.equals(""))
      {
        return Boolean.toString(Active);
      }
      else
      {
        if (Parameter.equalsIgnoreCase("true"))
        {
          Active = true;
          ResultCode = 0;
        }
        else if (Parameter.equalsIgnoreCase("false"))
        {
          Active = false;
        ResultCode = 0;
        }
      }
    } */

    if (ResultCode == 0)
    {
      getPipeLog().debug(LogUtil.LogECIPipeCommand(getSymbolicName(), getPipeName(), Command, Parameter));

      return "OK";
    }
    else
    {
      // This is not our event, pass it up the stack
      return super.processControlEvent(Command, Init, Parameter);
    }
  }
}
