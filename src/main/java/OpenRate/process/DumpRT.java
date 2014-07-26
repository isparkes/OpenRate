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

package OpenRate.process;

import OpenRate.CommonConfig;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.exception.InitializationException;
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
import OpenRate.record.IRecord;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * This class dumps the information passing through the pipe to a file, using
 * the record's knowledge of itself to format a list of values to dump. If the
 * dumping is not enabled, the records are passed directly through.
 *
 * Log access comes from the AbstractPlugIn class
 */
public class DumpRT
  extends AbstractPlugIn
{
  // This is the dump info that the record returns
  ArrayList<String> DumpInfo;

  // The defined types of dumping for this module
  private enum DumpType {ALL, ERRORS, FLAG, NONE};

  // tells us the current dump state
  private DumpType CurrentDumpType = DumpType.NONE;

  // The parts of the file names that we have recovered
  private String dumpLoggerName;

  // List of Services that this Client supports
  private final static String SERVICE_DUMPTYPE   = "DumpType";
  private final static String SERVICE_DUMPLOGGER = "DumpLogger";

 /**
  * Dump logger. This logger is used for logging dump information.
  */
  protected ILogger DumpLogger;

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited Plug In functions ---------------------
  // -----------------------------------------------------------------------------

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
  @Override
  public void init(String PipelineName, String ModuleName)
            throws InitializationException
  {
    String ConfigHelper;

    super.init(PipelineName,ModuleName);

    // Get the file definitions
    // ToDo: for the moment we get them without reference to the module
    // name meaning that there can only be one. We need to also refer to the
    // name of the module allowing more than one
    ConfigHelper = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(PipelineName,
                                                           ModuleName,
                                                           SERVICE_DUMPTYPE,
                                                           "false");
    processControlEvent(SERVICE_DUMPTYPE, true, ConfigHelper);

    ConfigHelper = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(PipelineName,
                                                           ModuleName,
                                                           SERVICE_DUMPLOGGER,
                                                           "None");

    // Check the result we got
    if (ConfigHelper.equals("None"))
    {
      message = "DumpRT must have a logger defined.";
      throw new InitializationException(message,getSymbolicName());
    }

    processControlEvent(SERVICE_DUMPLOGGER, true, ConfigHelper);
    // Get the dump type
    ConfigHelper = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(PipelineName,
                                                           ModuleName,
                                                           SERVICE_DUMPTYPE,
                                                           "false");
    processControlEvent(SERVICE_DUMPTYPE, true, ConfigHelper);
  }

  /**
  * This is called when the synthetic Header record is encountered, and has the
  * meaning that the stream is starting. In this case we have to open a new
  * dump file each time a stream starts.
  */
  @Override
  public IRecord procHeader(IRecord r)
  {
    r = procHeader(r);

    switch (CurrentDumpType)
    {
      case NONE:
      {
        // Do nothing
        break;
      }
      case ALL:
      case ERRORS:
      case FLAG:
      {
        // ****************** Create the Dump file *********************
        // Get the header dumop info
        DumpInfo = r.getDumpInfo();

        // dump it
        Iterator<String> DumpIter = DumpInfo.iterator();

        while (DumpIter.hasNext())
        {
          DumpLogger.info(DumpIter.next());
        }
      }
    }

    return r;
  }

  /**
  * This is called when an RT data record is encountered. You should do any normal
  * processing here.
  */
  @Override
  public IRecord procRTValidRecord(IRecord r)
  {
    switch (CurrentDumpType)
    {
      case NONE:
      case ERRORS:
      {
        // Do nothing
        break;
      }
      case ALL:
      {
        DumpInfo = r.getDumpInfo();

        // dump it
        Iterator<String> DumpIter = DumpInfo.iterator();

        while (DumpIter.hasNext())
        {
          DumpLogger.info(DumpIter.next());
        }
        break;
      }
      case FLAG:
      {
        // dump only in the case the flag is set
        if (r.isDump())
        {
          DumpInfo = r.getDumpInfo();

          // dump it
          Iterator<String> DumpIter = DumpInfo.iterator();

          while (DumpIter.hasNext())
          {
            DumpLogger.info(DumpIter.next());
          }
        }
      }
    }

    return r;
  }

 /**
  * This is called when a batch data record is encountered. We don't want to do
  * anything with this.
  */
  @Override
  public IRecord procValidRecord(IRecord r)
  {
    // pass back the record unmodified
    return r;
  }

 /**
  * This is called when an RT data record with errors is encountered. You should do
  * any processing here that you have to do for error records, e.g. statistics,
  * special handling, even error correction!
  */
  @Override
  public IRecord procRTErrorRecord(IRecord r)
  {
    switch (CurrentDumpType)
    {
      case NONE:
      {
        // Do nothing
        break;
      }
      case ERRORS:
      case ALL:
      {
        DumpInfo = r.getDumpInfo();

        // dump it
        Iterator<String> DumpIter = DumpInfo.iterator();

        while (DumpIter.hasNext())
        {
          DumpLogger.info(DumpIter.next());
        }
        break;
      }
      case FLAG:
      {
        // dump only in the case the flag is set
        if (r.isDump())
        {
          DumpInfo = r.getDumpInfo();

          // dump it
          Iterator<String> DumpIter = DumpInfo.iterator();

          while (DumpIter.hasNext())
          {
            DumpLogger.info(DumpIter.next());
          }
        }
      }
    }

    return r;
  }

 /**
  * This is called when a batch data record with errors is encountered. You should do
  * any processing here that you have to do for error records, e.g. statistics,
  * special handling, even error correction!
  */
  @Override
  public IRecord procErrorRecord(IRecord r)
  {
    // pass back the record unmodified
    return r;
  }

  /**
  * This is called when the synthetic trailer record is encountered, and has the
  * meaning that the stream is now finished. In this example, all we do is
  * pass the control back to the transactional layer.
  */
  @Override
  public IRecord procTrailer(IRecord r)
  {
    switch (CurrentDumpType)
    {
      case NONE:
      {
        // Do nothing
        break;
      }
      case ALL:
      case ERRORS:
      case FLAG:
      {
        DumpInfo = r.getDumpInfo();

        // dump it
        Iterator<String> DumpIter = DumpInfo.iterator();

        while (DumpIter.hasNext())
        {
          DumpLogger.info(DumpIter.next());
        }
      }
    }

    // now call the transaction maintenance
    r = procTrailer(r);

    return r;
  }

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------

 /**
  * processControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world, for
  * example turning the dumping on and off.
  *
  * @param Command The command that we are to work on
  * @param Init True if the pipeline is currently being constructed
  * @param Parameter The parameter value for the command
  * @return The result message of the operation
  */
  @Override
  public String processControlEvent(String Command, boolean Init, String Parameter)
  {

    int ResultCode = -1;

    if (Command.equalsIgnoreCase(SERVICE_DUMPTYPE))
    {
      if (Parameter.equalsIgnoreCase("all"))
      {
        CurrentDumpType = DumpType.ALL;
        ResultCode = 0;
      }
      else if (Parameter.equalsIgnoreCase("none"))
      {
        CurrentDumpType = DumpType.NONE;
        ResultCode = 0;
      }
      else if (Parameter.equalsIgnoreCase("errors"))
      {
        CurrentDumpType = DumpType.ERRORS;
        ResultCode = 0;
      }
      else if (Parameter.equalsIgnoreCase("flag"))
      {
        CurrentDumpType = DumpType.FLAG;
        ResultCode = 0;
      }
      else if (Parameter.equals(""))
      {
        switch(CurrentDumpType)
        {
          // Get the current state
          case ALL:
          {
            return "all";
          }
          case NONE:
          {
            return "none";
          }
          case ERRORS:
          {
            return "errors";
          }
          case FLAG:
          {
            return "flag";
          }
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_DUMPLOGGER))
    {
      if (Init)
      {
        dumpLoggerName = Parameter;
        DumpLogger = LogUtil.getLogUtil().getLogger(dumpLoggerName);
        ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return dumpLoggerName;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
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

    //Register this Client
    ClientManager.getClientManager().registerClient(getPipeName(),getSymbolicName(), this);

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_DUMPTYPE, ClientManager.PARAM_MANDATORY_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_DUMPLOGGER, ClientManager.PARAM_NONE);
  }
}
