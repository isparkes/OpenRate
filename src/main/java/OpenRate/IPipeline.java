/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2014.
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
package OpenRate;

import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.logging.ILogger;
import OpenRate.transaction.ISyncPoint;

/**
 * The IPipeline Abstraction is the container or implementor of the pipeline
 * application logic. It's principle reason for being is to allow multiple
 * pipelines to run within the same framework.
 *
 * Pipeline life cycle:
 *
 * init() ready for run run() running told to stop markForShutdown()
 * shutdownPipeline() cleanupPipeline()
 */
public interface IPipeline
        extends Runnable, ISyncPoint {

  /**
   * Init method will be called prior to run so that we set up the pipeline and
   * instantiate and initialise all the modules.
   *
   * @param PipelineName The name of this pipeline
   * @throws OpenRate.exception.InitializationException
   */
  public void init(String PipelineName)
          throws InitializationException;

  /**
   * Perform pipeline plugin level close down processing.
   */
  public void shutdownPipeline();

  /**
   * Perform any process level cleanup required. This should take care of
   * de-referencing and closing process level things
   */
  public void cleanupPipeline();

  /**
   * stop the process. This can be called to safely shutdown a process that is
   * either long running or runs continuously. It's not intended to be a drastic
   * shutdown, but rather a "find a reasonable spot to stop & do so" type
   * message. A few minutes of processing before reaching such a point is, while
   * not recommended, not unreasonable in certain cases. The process should make
   * an effort to shutdown as quickly as is reasonable without leaving the
   * application in an invalid state.
   */
  public void markForShutdown();

  /**
   * Return the symbolic name of the pipe
   */
  @Override
  public String getSymbolicName();

  /**
   * Returns true if the pipe is a batch pipeline
   *
   * @return true if the pipeline is a batch pipeline, false if it is real time
   */
  public boolean isBatchPipeline();

  /**
   * Used for processing schedule management - set the pipe to the fast schedule
   */
  public void setSchedulerHigh();

  /**
   * Used for processing schedule management - see if the pipe is in the fast
   * schedule
   *
   * @return true if we are in the fast schedule
   */
  public boolean getSchedulerHigh();

  /**
   * Returns true if the pipe aborted
   *
   * @return true if the pipeline aborted
   */
  public boolean isAborted();

  /**
   * Returns the pipeline logger.
   *
   * @return The logger for the pipeline
   */
  public ILogger getPipeLog();

  /**
   * Returns the pipeline exception handler.
   *
   * @return The exception handler for the pipeline
   */
  public ExceptionHandler getPipelineExceptionHandler();
}
