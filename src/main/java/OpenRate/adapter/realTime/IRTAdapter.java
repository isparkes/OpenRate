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
package OpenRate.adapter.realTime;

import OpenRate.CommonConfig;
import OpenRate.adapter.IAdapter;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.process.IPlugIn;
import OpenRate.record.FlatRecord;
import OpenRate.record.IRecord;
import java.util.ArrayList;

/**
 * The IOutputAdapter is responsible for taking a completed
 * work set and storing it.
 */
public interface IRTAdapter
  extends IAdapter,
          Runnable
{
 /**
  * The tag that lets us get the batch size setting.
  */
  public static final String BATCH_SIZE = CommonConfig.BATCH_SIZE;

 /**
  * The default value of the batcgh size.
  */
  public static final String DEFAULT_BATCH_SIZE = CommonConfig.DEFAULT_BATCH_SIZE;

// ************************ Initialisation Stuff *******************************

 /**
  * Initialise the module. Called during pipeline creation.
  *
  * @param PipelineName The name of the pipeline this module is in
  * @param ModuleName The module symbolic name of this module
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String PipelineName, String ModuleName)
            throws InitializationException;

// ************************ Input Adapter Stuff ********************************

 /**
  * This method is used to serialise the calls from various listener threads,
  * putting them in order, and requesting that the user level implementation
  * layer parse the records. This method is implemented as a synchronised
  * method, enforcing an ordering on the records.
  *
  * @param RTRecordToProcess The Real Time record to process
  * @return The processed real time record
  * @throws ProcessingException
  */
  public IRecord performInputMapping(FlatRecord RTRecordToProcess) throws ProcessingException;

// ************************ Output Adapter Stuff *******************************

  /**
   * Processing method for the output adapter. Opens the possibility
   * of processing completed records asynchronously while the pipeline
   * is running. It can also be run directly by the exec strategy from
   * within the main thread. Both modes are supported and reasonable.
   */
  @Override
  public void run();

  /**
   * reset the plug in to ensure that it's ready to process records again after
   * it has been exited by calling markForExit().
   */
  public void reset();

  /**
   * Mark output adapter complete so that it exits at the next possibility
   */
  public void markForClosedown();

  /**
   * Close is called outside of the strategy to allow the output
   * adapter to commit any work that should only be done once per
   * interface run. NOT once per batch cycle like flush()
   *
   * @throws OpenRate.exception.ProcessingException
   */
  public void close()
    throws ProcessingException;

  /**
   * Perform any cleanup. Called by the OpenRateApplication during application
   * shutdown. This should do any final cleanup and closing of resources.
   * Note: It is not called during normal processing, so it's only useful for
   * true shutdown logic.
   *
   * If you need something to happen during normal pipeline execution, use
   * <code>completeBatch()</code>.
   */
  @Override
  public void cleanup();

 /**
  * process a record using Real Time mode
  *
  * @param recordToProcess The record we are to process
  * @return The processed record
  * @throws ProcessingException
  */
  public IRecord processRTRecord(IRecord recordToProcess) throws ProcessingException;

 /**
  * process a record using Real Time mode
  *
  * @param recordToProcess The record we are to process
  * @return The processed record
  * @throws ProcessingException
  */
  public FlatRecord processRTRecord(FlatRecord recordToProcess) throws ProcessingException;

  /**
   * Performs any final shutdown activities for this adapter.
   */
  public void closeStream();

 /**
  * return the symbolic name
  *
  * @return The current module symbolic name
  */
  public String getSymbolicName();

 /**
  * set the symbolic name
  *
  * @param name The symbolic name to set
  */
  public void setSymbolicName(String name);

  /**
   * Set the list of plugins for this piplines, in order that we may process
   * a record down the chain at will
   *
   * @param PlugInList The list of plugins in the pipe
   */
  public void setProcessingList(ArrayList<IPlugIn> PlugInList);

  /**
   * Perform the output processing on a valid record
   *
   * @param RTRecordToProcess The record to perform the mapping on
   * @return The mapped flat record
   * @throws ProcessingException
   */
  public FlatRecord performValidOutputMapping(IRecord RTRecordToProcess) throws ProcessingException;

  /**
   * Perform the output processing on an error record
   *
   * @param RTRecordToProcess The record to perform the mapping on
   * @return The mapped flat record
   * @throws ProcessingException
   */
  public FlatRecord performErrorOutputMapping(IRecord RTRecordToProcess) throws ProcessingException;
}
