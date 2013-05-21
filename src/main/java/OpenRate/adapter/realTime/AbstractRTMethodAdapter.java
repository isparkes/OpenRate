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

import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.FlatRecord;
import OpenRate.record.IRecord;

/**
 * This class implements a socket listener based on the the real time (RT)
 * adapter for the OpenRate framework. This adapter handles real time events
 * coming from socket sources.
 */
public abstract class AbstractRTMethodAdapter extends AbstractRTAdapter
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: AbstractRTMethodAdapter.java,v $, $Revision: 1.15 $, $Date: 2013-05-13 18:12:12 $";

  // Holds the reference to the raw processing method
  private static IRTAdapter processingAdapter;

  /**
   * Used to link directly into the raw processing pipe
   * 
   * @return the reference to the processing adapter
   */
  public static IRTAdapter getProcessingAdapter()
  {
    return processingAdapter;
  }

 /**
  * Constructor
  */
  public AbstractRTMethodAdapter()
  {
    super();

    processingAdapter = this;
  }

 /**
  * Initialise the module. Called during pipeline creation.
  *
  * @param PipelineName The name of the pipeline this module is in
  * @param ModuleName The module symbolic name of this module
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String PipelineName, String ModuleName) throws InitializationException
  {
    // Perform parent processing first
    super.init(PipelineName, ModuleName);
  }

 /**
  * Create the listener object, which will be used to allow the communication
  * with the client tasks.
  */
  @Override
  public void initialiseInputListener()
  {
    // Create the communication objects
  }

  @Override
  public void shutdownInputListener()
  {
    // Nothing
  }

 /**
  * Stubbed out methods for mapping. Override if you want to use them. It is
  * unlikely, because the method based processing can create records of the
  * correct format directly, whereas socket processing receives the information
  * in a string format therefore forcing the conversion from flat data to
  * record data.
  *
  * @param RTRecordToProcess Nothing
  * @return Nothing
  * @throws ProcessingException
  */
  @Override
  public FlatRecord performValidOutputMapping(IRecord RTRecordToProcess) throws ProcessingException {
    throw new UnsupportedOperationException("Not supported.");
  }

 /**
  * Stubbed out methods for mapping. Override if you want to use them. It is
  * unlikely, because the method based processing can create records of the
  * correct format directly, whereas socket processing receives the information
  * in a string format therefore forcing the conversion from flat data to
  * record data.
  *
  * @param RTRecordToProcess Nothing
  * @return Nothing
  */
  @Override
  public FlatRecord performErrorOutputMapping(IRecord RTRecordToProcess) {
    throw new UnsupportedOperationException("Not supported.");
  }
}
