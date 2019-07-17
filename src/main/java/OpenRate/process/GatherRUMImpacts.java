

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
      tmpResID = tmpCP.resCounter;

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
            tmpImpactsRider.child.ResourceID = tmpCP.resCounter;
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
      getPipeLog().debug(LogUtil.LogECIPipeCommand(getSymbolicName(), getPipeName(), Command, Parameter));

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
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_0_BAL_IMP, ClientManager.PARAM_NONE);
  }
}
