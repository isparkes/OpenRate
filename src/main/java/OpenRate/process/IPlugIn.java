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

import OpenRate.buffer.IConsumer;
import OpenRate.buffer.ISupplier;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.IRecord;

/**
 * IPlugIn Interface. This defines the basic elements needed for processing
 * records in a chained manner. The implementations of this will deal with the
 * actual task of moving the data around.
 */
public interface IPlugIn extends Runnable
{
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
  public void init(String PipelineName, String ModuleName) throws InitializationException;

 /**
  * do the actual processing for the plug in
  *
  * @throws OpenRate.exception.ProcessingException
  */
  public void process() throws ProcessingException;

 /**
  * Count the number of records waiting at the output of this plugin
  *
  * @return The number of records in this batch
  */
  public int getOutboundRecordCount();

  /**
  * Shuts down the PlugIn. Use this to save any configuration or data before
  * the plug in closes
  */
  public void shutdown();

  /**
   * Set the inbound delivery mechanism.
   *
   * @param c The supplier
   */
  public void setInbound(ISupplier c);

 /**
  * Set the outbound delivery mechanism.
  *
  * @param c The consumer
  */
  public void setOutbound(IConsumer c);

 /**
  * Set the holding bin used for errored records.
  *
  * @param err The consumer for error records
  */
  public void setErrorBuffer(IConsumer err);

 /**
  * Set the handler used for reporting fatal errors during IPlugIn
  * processing. Since each IPlugIn is run within a thread, the call stack is
  * not an appropriate mechanism for reporting critical processing errors.
  * Therefore, a Handler object is provided for the Plug In to use for
  * logging unrecoverable processing errors.
  * Note: This handler must assume that any error is fatal. Non-fatal errors
  * should be handled by adding errors to the record(s) being processed.
  *
  * @param h The parent assigned handler
  */
  public void setExceptionHandler(ExceptionHandler h);

 /**
  * Tag this plug in to shutdown. Implementation may vary. For example, a
  * plug in may be marked for exit, but not actually shutdown until it detects
  * a certain number of empty cycles.
  */
  public void markForShutdown();

 /**
  * reset the plug in to ensure that it's ready to process records again after
  * it has been exited by calling markForExit().
  */
  public void reset();

 /**
  * Return the suggested number of threads to launch for this
  * IPlugIn. Obviously this requires that the application uses
  * a Pipeline that implements multi-threading.
  *
  * @return The number of threads in use
  */
  public int numThreads();

 /**
  * return the symbolic name
  *
  * @return The symbolic name
  */
  public String getSymbolicName();

 /**
  * set the symbolic name
  * @param name The symbolic name
  */
  public void setSymbolicName(String name);

 /**
  * This is called when a RT data record is encountered. You should do any normal
  * processing here. For most purposes this is steered to the normal (batch)
  * processing, but this can be overwritten
  *
  * @param r The record we are working on
  * @return The processed record
  * @throws ProcessingException
  */
  public IRecord procRTValidRecord(IRecord r) throws ProcessingException;

 /**
  * This is called when a RT data record with errors is encountered. You should do
  * any processing here that you have to do for error records, e.g. statistics,
  * special handling, even error correction! For most purposes this is steered
  * to the normal (batch) processing, but this can be overwritten
  *
  * @param r The record we are working on
  * @return The processed record
  * @throws ProcessingException
  */
  public IRecord procRTErrorRecord(IRecord r) throws ProcessingException;
}
