
package OpenRate.adapter.realTime;

import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.FlatRecord;
import OpenRate.record.IRecord;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * This class implements a socket listener based on the the real time (RT)
 * adapter for the OpenRate framework. This adapter handles real time events
 * coming from socket sources.
 *
 * The task of this layer is to create the listening and marshalling
 * infrastructure.
 */
public abstract class AbstractRTSocketAdapter extends AbstractRTAdapter
{
  // the port number to listen on
  int listenerPort;

  // This is the listener socket server thread, which creates and spawns the
  // individual listener threads as connections are opened
  private SocketServerThread RTSocketServer;

  // Thread group for managing all the listeners
  private final ArrayList<SocketServerThread> listenerGroup = new ArrayList<>();

 /**
  * Constructor
  */
  public AbstractRTSocketAdapter()
  {
    super();
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
    String ConfigHelper;

    // Perform parent processing first
    super.init(PipelineName, ModuleName);

    // Get the port number
    ConfigHelper = PropertyUtils.getPropertyUtils().getRTAdapterPropertyValueDef(PipelineName, ModuleName, "ListenerPort", "0");

    if (ConfigHelper.equals("0"))
    {
      throw new InitializationException ("Please set the port number to listen on using the ListenerPort property",getSymbolicName());
    }

    // see if we can convert it
    try
    {
      listenerPort = Integer.parseInt(ConfigHelper);
    }
    catch (NumberFormatException nfe)
    {
      // Could not use the value we got
      throw new InitializationException ("Could not parse the ListenerPort value <" + ConfigHelper + ">",getSymbolicName());
    }
  }

 /**
  * Start the listener which allocates and manages the threads. This allows
  * multi-thread processing for this interface.
  */
  @Override
  public void initialiseInputListener()
  {
    // start the socket server thread, that will accept incoming connections
    RTSocketServer = new SocketServerThread();

    // will be handling threads
    RTSocketServer.setParentAdapter(this);
    RTSocketServer.setPipelineLog(getPipeLog());
    RTSocketServer.setPort(listenerPort);
    RTSocketServer.setPipelineName(getPipeName());
    RTSocketServer.setThreadId("RTSocketServer");

    // start thread in the listenerGroup thread group
    Thread socketThread = new Thread(RTSocketServer, "RTSocketServer");

    // Add the thread to the map - used for management and shutting down
    listenerGroup.add(RTSocketServer);

    // Start the listener
    socketThread.start();
  }

  @Override
  public void shutdownInputListener()
  {
    Iterator<SocketServerThread> threadGroupIterator = listenerGroup.iterator();

    // Shutdown all of the threads
    while (threadGroupIterator.hasNext())
    {
      SocketServerThread tmpSocketThread = threadGroupIterator.next();
      tmpSocketThread.markForClosedown();
    }
  }

 /**
  * This method takes the incoming real time record, and prepares it for
  * submission into the processing pipeline.
  *
  * @param RTRecordToProcess The real time record to map
  * @return The mapped real time record
  * @throws ProcessingException
  */
  @Override
  public synchronized IRecord performInputMapping(FlatRecord RTRecordToProcess) throws ProcessingException
  {
    IRecord tmpRecord;

    // add the record ID and the RT indicator
    tmpRecord = procInputValidRecord((IRecord) RTRecordToProcess);

    if (tmpRecord != null)
    {
      tmpRecord.setRealtime(true);
      tmpRecord.setRecordID(currRecordNumber);
      currRecordNumber++;
    }

    return tmpRecord;
  }

 /**
  * This method takes the incoming real time record, and prepares it for
  * submission into the processing pipeline.
  *
  * @param RTRecordToProcess The real time record to process
  * @return The processed real time record
  */
  @Override
  public synchronized FlatRecord performValidOutputMapping(IRecord RTRecordToProcess)
  {
    IRecord tmpRecord;

    // marshall the events to the valid or error output
    tmpRecord = procOutputValidRecord((IRecord) RTRecordToProcess);

    // Copy over the record ID
    tmpRecord.setRecordID(RTRecordToProcess.getRecordID());

    return (FlatRecord) tmpRecord;
  }

 /**
  * This method takes the incoming real time record, and prepares it for
  * submission into the processing pipeline.
  *
  * @param RTRecordToProcess The real time record to process
  * @return The processed real time record
  */
  @Override
  public synchronized FlatRecord performErrorOutputMapping(IRecord RTRecordToProcess)
  {
    IRecord tmpRecord;

    // marshall the events to the valid or error output
    tmpRecord = procOutputErrorRecord((IRecord) RTRecordToProcess);

    // Copy over the record ID
    tmpRecord.setRecordID(RTRecordToProcess.getRecordID());

    return (FlatRecord) tmpRecord;
  }
}
