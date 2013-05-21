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

package OpenRate.process;

import OpenRate.configurationmanager.ClientManager;
import OpenRate.exception.InitializationException;
import OpenRate.logging.LogUtil;
import OpenRate.record.*;

/**
 * This class provides the abstract base for a more complex rating plug in. A
 * raw rate object is retrieved from the RateCache object, and this class
 * provides the primitives required for performing rating, including selecting
 * the correct RUM (Rateable Usage Metric) to use and creating balance impacts
 * on the record.
 *
 * This is a high-level module that requires records based on the RatingRecord
 * class.
 */
public class GatherRUMImpacts extends AbstractStubPlugIn
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: GatherRUMImpacts.java,v $, $Revision: 1.25 $, $Date: 2013-05-13 18:12:10 $";

  // List of Services that this Client supports
  private static final String SERVICE_0_BAL_IMP = "CreateZeroBalImpacts";

  // Whether we create bal impact packets for 0 balance items
  private boolean Create0BalImpacts = false;

  private class SummarizationArray
  {
    String ResourceName = null;
    double ResourceValue = 0;
    int    CounterPeriod = 0;
    int    ResourceID = 0;
    SummarizationArray child = null;
  }

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited Plug In functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * This is called when a good record is encountered. We perform the charge
  * packet summarization in this case.
  *
  * @param r The record to work on
  * @return Modified record
  */
  @Override
  public IRecord procValidRecord(IRecord r)
  {
    SummarizeChargePackets((RatingRecord)r);

    return r;
  }

 /**
  * This is called when an error record is encountered. We do no processing in
  * this case.
  *
  * @param r The record to work on
  * @return Modified record
  */
  @Override
  public IRecord procErrorRecord(IRecord r)
  {
    return r;
  }

  // -----------------------------------------------------------------------------
  // ----------------------- Start of utility functions --------------------------
  // -----------------------------------------------------------------------------

  /**
   * Calculate the summary of the charge packets in the record
   *
   * @param CurrentRecord The record to summarise
   */
  public void SummarizeChargePackets(RatingRecord CurrentRecord)
  {
    int Index;
    ChargePacket tmpCP;
    String tmpResource;
    int    tmpResID;
    boolean found;
    boolean createImpact;
    SummarizationArray tmpImpacts = new SummarizationArray();
    tmpImpacts.ResourceName = "root";
    SummarizationArray tmpImpactsRider;
    BalanceImpact tmpBalImpact;

    // Cycle over the charge packets gathering the impacts as we go
    for (Index = 0 ; Index < CurrentRecord.getChargePacketCount() ; Index++)
    {
      tmpCP = CurrentRecord.getChargePacket(Index);

      tmpResource = tmpCP.resource;
      tmpResID = tmpCP.ResCounter;

      // if the resource is not set, we cannot do anything with the record
      if (tmpResource == null)
      {
        CurrentRecord.addError(new RecordError("ERR_RESOURCE_NOT_SET",ErrorType.DATA_NOT_FOUND, getSymbolicName()));
        return;
      }

      // initialise
      found = false;

      // reset the rider object
      tmpImpactsRider = tmpImpacts;
      do
      {
        if ((tmpImpactsRider.ResourceName.equals(tmpResource)) &
            (tmpImpactsRider.CounterPeriod == CurrentRecord.getCounterCycle()) &
             (tmpImpactsRider.ResourceID == tmpResID))
        {
          tmpImpactsRider.ResourceValue += tmpCP.chargedValue;
          break;
        }

        if (tmpImpactsRider.child == null)
        {
          // we are at the end of the list
          if (!found)
          {
            // we did not find an impact to aggregate into, so make one
            tmpImpactsRider.child = new SummarizationArray();
            tmpImpactsRider.child.ResourceName = tmpResource;
            tmpImpactsRider.child.ResourceValue = tmpCP.chargedValue;
            tmpImpactsRider.child.ResourceID = tmpCP.ResCounter;
            tmpImpactsRider.child.CounterPeriod = CurrentRecord.getCounterCycle();
            break;
          }
        }
        else
        {
          // move down the list
          tmpImpactsRider = tmpImpactsRider.child;
        }
      } while (tmpImpactsRider != null);
    }

    // We have finished gathering, now create the balance impacts
    tmpImpactsRider = tmpImpacts;
    while (tmpImpactsRider.child != null)
    {
      // find out whether we are to create the impact or not
      createImpact = (tmpImpactsRider.child.ResourceValue != 0) | Create0BalImpacts;

      if (createImpact)
      {
        tmpBalImpact = new BalanceImpact();

        tmpBalImpact.type = "R";
        tmpBalImpact.balanceDelta = tmpImpactsRider.child.ResourceValue;
        tmpBalImpact.Resource = tmpImpactsRider.child.ResourceName;
        tmpBalImpact.counterID = tmpImpactsRider.child.ResourceID;
        tmpBalImpact.recID = tmpImpactsRider.child.CounterPeriod;

        CurrentRecord.addBalanceImpact(tmpBalImpact);
      }

      // move down the list
      tmpImpactsRider = tmpImpactsRider.child;
    }
  }
  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------

  /**
  * processControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world.
   *
   * @param Command The Command that should be processed
   * @param Init True if we are during pipeline starup
   * @param Parameter The parameter value that should be operated on
   * @return The message as a result of the operation
   */
  @Override
  public String processControlEvent(String Command, boolean Init,
                                    String Parameter)
  {
    int ResultCode = -1;

    // The input file name can be changed
    if (Command.equalsIgnoreCase(SERVICE_0_BAL_IMP))
    {
        if (Parameter.equalsIgnoreCase("true"))
        {
          Create0BalImpacts = true;
          ResultCode = 0;
        }
        else if (Parameter.equalsIgnoreCase("false"))
        {
          Create0BalImpacts = false;
          ResultCode = 0;
        }
        else if (Parameter.equals(""))
        {
            // Get the current state
            if (Create0BalImpacts)
            {
              return "true";
            }
            else
            {
              return "false";
            }
        }
        else
        {
          // we don't recognise this, give an error
          return "Unknown parameter value.";
        }
      }

    if (ResultCode == 0)
    {
      pipeLog.debug(LogUtil.LogECIPipeCommand(getSymbolicName(), pipeName, Command, Parameter));

      return "OK";
    }
    else
    {
      // This is not our event, pass it up the stack
      return super.processControlEvent(Command, Init, Parameter);
    }
  }

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
    super.registerClientManager();

    //Register services for this Client
    ClientManager.registerClientService(getSymbolicName(), SERVICE_0_BAL_IMP, ClientManager.PARAM_NONE);
  }
}
