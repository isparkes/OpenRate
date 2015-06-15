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
