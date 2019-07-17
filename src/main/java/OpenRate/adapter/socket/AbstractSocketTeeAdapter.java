

package OpenRate.adapter.socket;

import OpenRate.adapter.objectInterface.AbstractTeeAdapter;
import OpenRate.adapter.realTime.TeeBatchConverter;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.exception.InitializationException;
import OpenRate.logging.LogUtil;
import OpenRate.record.*;
import OpenRate.utils.PropertyUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Iterator;

/**
 * The buffer input adapter allows us to link pipelines together using buffers.
 * In particular, this is useful for adding persistence to a real time pipeline
 * where the writing to a table or file should happen after real time processing
 * has happened.
 *
 * This module tees into a Real Time pipeline and takes a feed of the events
 * for putting into a batch pipeline. This is usually used for persistence
 * of RT events in a batch mode, however, it can also be used for balance
 * updates in a batch pipeline.
 *
 * Socket Input Adapter
 * --------------------
 * The output of the socket tee adapter allows you to "sniff" events out of
 * a pipeline (real time or batch) and put them into another pipeline (batch)
 * for further processing.
 *
 * Input >->->- Pipeline 1 ->->->- Socket Tee Adapter ->->-> Output
 *                                     |
 *   +------------- TCPIP -------------+
 *   |
 *   +-> Socket Input Adapter >->- Pipeline 2 ->->->-> Output
 *
 */
public abstract class AbstractSocketTeeAdapter
        extends AbstractTeeAdapter
{
  private final static String SERVICE_BATCHHOST  = "BatchHost";
  private final static String SERVICE_BATCHPORT  = "BatchPort";

  // the socket we connect to the batch pipeline with
  private Socket smtpSocket;

  // the host to communicate with
  private String batchHost;

  // the port on the host to communicate with
  private int batchPort;

  @Override
  public void init(String PipelineName, String ModuleName) throws InitializationException
  {
    String ConfigHelper;

    super.init(PipelineName, ModuleName);

    // Create the batch converter if we need it
    batchConv = new TeeBatchConverter();

    ConfigHelper = initGetBatchHost();
    if (ConfigHelper.equalsIgnoreCase("NONE"))
    {
      message = "No host defined for the batch listener";
      throw new InitializationException(message,getSymbolicName());
    }
    else
    {
      processControlEvent(SERVICE_BATCHHOST, true, ConfigHelper);
    }

    ConfigHelper = initGetBatchPort();
    if (ConfigHelper.equalsIgnoreCase("NONE"))
    {
      message = "No host port defined for the batch listener";
      throw new InitializationException(message,getSymbolicName());
    }
    else
    {
      processControlEvent(SERVICE_BATCHPORT, true, ConfigHelper);
    }
  }

 /**
  * Push the collected batch of records into the transport layer
  *
  * @param batchToPush The batch we are pushing
  */
  @Override
  public void pushTeeBatch(Collection<IRecord> batchToPush)
  {
    PrintWriter os = null;
    FlatRecord tmpFlatRecord;
    boolean Ready;
    int batchCount = 0;

    // now pull back out
    Iterator<IRecord> iter = batchToPush.iterator();

    Ready = true;
    BufferedReader is;
    while (iter.hasNext() & Ready)
    {
      AbstractRecord tmpRecord = (AbstractRecord) iter.next();
      if (tmpRecord instanceof HeaderRecord)
      {
        try {
          // send records to the output
          smtpSocket = new Socket(batchHost, batchPort);
        }
        catch (UnknownHostException ex)
        {
          System.err.println("Unknown host");
        }
        catch (IOException ex)
        {
          System.err.println("IO exception");
        }

        try {
          os = new PrintWriter(smtpSocket.getOutputStream(), true);
        }
        catch (IOException ex)
        {
          System.err.println("PW IO exception");
        }

        try {
          is = new BufferedReader(new InputStreamReader(smtpSocket.getInputStream()));
        }
        catch (IOException ex)
        {
          System.err.println("BR IO exception");
        }

        // send the header
        os.println("HEADER");
        batchCount = 0;
      }
      else if (tmpRecord instanceof FlatRecord)
      {
        tmpFlatRecord = (FlatRecord) tmpRecord;
        os.println(tmpFlatRecord.getData());
        batchCount++;
      }
      else if (tmpRecord instanceof TrailerRecord)
      {
        // send the header
        System.out.println("Send Batch count = " + batchCount);
        os.println("TRAILER");

        os.flush();
        os.close();
        try {
          smtpSocket.close();
        } catch (IOException ex)
        {
          System.err.println("SC IO exception");
        }
      }
    }
  }

  // -----------------------------------------------------------------------------
  // ----------------- Start of published hookable functions ---------------------
  // -----------------------------------------------------------------------------



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
    ClientManager.getClientManager().registerClient(getPipeName(),getSymbolicName(), this);

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_BATCHHOST, ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_BATCHPORT, ClientManager.PARAM_MANDATORY);
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

    if (Command.equalsIgnoreCase(SERVICE_BATCHHOST))
    {
      if (batchConv == null)
      {
        return "Real Time Batch Converter is not active";
      }

      if (Parameter.equals(""))
      {
        return batchHost;
      }
      else
      {
        batchHost = Parameter;

        ResultCode = 0;
      }
    }
    int BatchPort;

    if (Command.equalsIgnoreCase(SERVICE_BATCHPORT))
    {
      if (batchConv == null)
      {
        return "Real Time Batch Converter is not active";
      }

      if (Parameter.equals(""))
      {
        return Integer.toString(batchPort);
      }
      else
      {
        try
        {
          BatchPort = Integer.parseInt(Parameter);
          batchPort = BatchPort;
        }
        catch (NumberFormatException nfe)
        {
          getPipeLog().error("Invalid number for batch port. Passed value = <" +
                Parameter + ">");
        }

        ResultCode = 0;
      }
    }

    if (ResultCode == 0)
    {
      getPipeLog().debug(LogUtil.LogECIPipeCommand(getSymbolicName(), getPipeName(), Command, Parameter));

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
  private String initGetBatchHost() throws InitializationException
  {
    String tmpValue;
    tmpValue = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(getPipeName(),getSymbolicName(),
                                                   SERVICE_BATCHHOST, "NONE");

    return tmpValue;
  }

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetBatchPort() throws InitializationException
  {
    String tmpValue;
    tmpValue = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(getPipeName(),getSymbolicName(),
                                                   SERVICE_BATCHPORT, "NONE");

    return tmpValue;
  }
}
