/* ====================================================================
 * Limited Evaluation License:
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

package OpenRate.adapter.objectInterface;

import OpenRate.adapter.realTime.TeeBatchConverter;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.process.AbstractPlugIn;
import OpenRate.record.FlatRecord;
import OpenRate.record.IRecord;
import OpenRate.utils.PropertyUtils;
import java.util.Collection;

/**
 * This module tees into a Real Time pipeline and takes a feed of the events
 * for putting into a batch pipeline. This is usually used for persistence
 * of RT events in a batch mode, however, it can also be used for balance
 * updates in a batch pipeline.
 * 
 * Buffer Input Adapter
 * --------------------
 * The output of the buffer tee adapter allows you to "sniff" events out of
 * a pipeline (real time or batch) and put them into another pipeline (batch)
 * for persistence or further processing. This converts synchronous events into
 * asynchronous events. The coupling between the two pipelines is a normal FIFO 
 * buffer. The buffer is accessed using a "LinkedBufferCache" which allows the 
 * FIFO to be set up and accessed by name.
 * 
 * Input >->->- Pipeline 1 ->->->- Buffer Tee Adapter ->->-> Output
 *                                     |
 *   +------------- Buffer ------------+
 *   |
 *   +-> Buffer Input Adapter >->- Pipeline 2 ->->->-> Output
 * 
 * 
 * Socket Input Adapter
 * --------------------
 * The output of the socket tee adapter allows you to "sniff" events out of
 * a pipeline (realtime or batch) and put them into another pipeline (batch)
 * for further processing.
 * 
 * Input >->->- Pipeline 1 ->->->- Socket Tee Adapter ->->-> Output
 *                                     |
 *   +------------- TCPIP -------------+
 *   |
 *   +-> Socket Input Adapter >->- Pipeline 2 ->->->-> Output
 * 
 */
public abstract class AbstractTeeAdapter 
        extends AbstractPlugIn
        implements ITeeAdapter
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: AbstractTeeAdapter.java,v $, $Revision: 1.14 $, $Date: 2013-05-13 18:12:12 $";

  private final static String SERVICE_CONVBATCH  = "ConversionBatchSize";
  private final static String DEFAULT_CONVBATCH  = "1000";
  private final static String SERVICE_PURGETIME  = "PurgeTime";
  private final static String DEFAULT_PURGETIME  = "5000";

  /**
   * This is the local variable that we use to determine the batch size
   */
  protected int BatchSize;

  /**
   * the batch converter thread
   */
  protected TeeBatchConverter batchConv;

 /**
  * This is the local variable that we use to determine the buffer high water
  * mark
  */
  protected int purgeTime;

  @Override
  public void init(String PipelineName, String ModuleName) throws InitializationException
  {
    String ConfigHelper;

    super.init(PipelineName, ModuleName);

    // Create the batch converter if we need it
    batchConv = new TeeBatchConverter();

    // Get the batch size we should be working on
    ConfigHelper = initGetConvBatchSize();
    processControlEvent(SERVICE_CONVBATCH, true, ConfigHelper);
    ConfigHelper = initGetPurgeTime();
    processControlEvent(SERVICE_PURGETIME, true, ConfigHelper);

    // launch it in it's own thread
    Thread batchConvThread = new Thread(batchConv, pipeName + "-BatchConverter");

    batchConv.setParentAdapter(this);

    // Start the thread
    batchConvThread.start();
  }

  @Override
  public IRecord procHeader(IRecord r)
  {
    return r;
  }

  @Override
  public IRecord procValidRecord(IRecord r) throws ProcessingException
  {
    FlatRecord tmpRecord = (FlatRecord) performValidOutputMapping(r);

    if (tmpRecord != null)
    {
      addRecordToOutputBatch(tmpRecord);
    }

    return r;
  }

  @Override
  public IRecord procErrorRecord(IRecord r) throws ProcessingException
  {
    FlatRecord tmpRecord = (FlatRecord) performErrorOutputMapping(r);

    if (tmpRecord != null)
    {
      addRecordToOutputBatch(tmpRecord);
    }

    return r;
  }

  @Override
  public IRecord procTrailer(IRecord r)
  {
    return r;
  }

  /**
   * Places a record into the output buffer for processing by a batch adapter
   * using socket communication.
   *
   * @param tmpRecord The record to write
   */
  public void addRecordToOutputBatch(IRecord tmpRecord)
  {
    batchConv.addRecordToOutputBatch(tmpRecord, false);
  }

 /**
  * Push the collected batch of records into the transport layer
  *
  * @param batchToPush The batch we are pushing
  */
  @Override
  public abstract void pushTeeBatch(Collection<IRecord> batchToPush);

  // ----------------------------------------------------------------------------
  // ----------------- Start of published hookable functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * This method takes the outgoing real time record, and prepares it for
  * returning to the submitter.
  *
  * @param RTRecordToProcess The real time record to process
  * @return The processed real time record
  * @throws ProcessingException
  */
  public abstract IRecord performValidOutputMapping(IRecord RTRecordToProcess) throws ProcessingException;

 /**
  * This method takes the outgoing real time record, and prepares it for
  * returning to the submitter.
  *
  * @param RTRecordToProcess The real time record to process
  * @return The processed real time record
  */
  public abstract IRecord performErrorOutputMapping(IRecord RTRecordToProcess);

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
    // Set the client reference and the base services first
    ClientManager.registerClient(pipeName,getSymbolicName(), this);

    //Register services for this Client
    ClientManager.registerClientService(getSymbolicName(), SERVICE_CONVBATCH, ClientManager.PARAM_MANDATORY);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_PURGETIME, ClientManager.PARAM_MANDATORY);
  }

  /**
  * processControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world.
   *
  * @param Command The command that we are to work on
  * @param Init True if the pipeline is currently being constructed
  * @param Parameter The parameter value for the command
  * @return The result message of the operation
   */
  @Override
  public String processControlEvent(String Command, boolean Init,
                                    String Parameter)
  {
    int ResultCode = -1;

    if (ResultCode == 0)
    {
      pipeLog.debug(LogUtil.LogECIPipeCommand(getSymbolicName(), pipeName, Command, Parameter));

      return "OK";
    }
    else
    {
      return super.processControlEvent(Command, Init, Parameter);
    }
  }
  // -----------------------------------------------------------------------------
  // -------------------- Start of local utility functions -----------------------
  // -----------------------------------------------------------------------------

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetConvBatchSize() throws InitializationException
  {
    String tmpValue;
    tmpValue = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(pipeName,getSymbolicName(),
                                                   SERVICE_CONVBATCH, DEFAULT_CONVBATCH);

    return tmpValue;
  }

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetPurgeTime() throws InitializationException
  {
    String tmpValue;
    tmpValue = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(pipeName,getSymbolicName(),
                                                   SERVICE_PURGETIME, DEFAULT_PURGETIME);

    return tmpValue;
  }
}
