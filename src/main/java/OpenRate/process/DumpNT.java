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

import OpenRate.CommonConfig;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;
import OpenRate.utils.PropertyUtils;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;


/**
 * This class dumps the information passing through the pipe to a file, using
 * the record's knowledge of itself to format a list of values to dump. If the
 * dumping is not enabled, the records are passed directly through.
 *
 * Log access comes from the AbstractPlugIn class
 */
public class DumpNT
  extends AbstractPlugIn
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: DumpNT.java,v $, $Revision: 1.49 $, $Date: 2013-05-13 18:12:10 $";

  // This is the dump info that the record returns
  ArrayList<String> DumpInfo;

  private enum DumpType {ALL, ERRORS, FLAG, NONE};

  // tells us the current dump state
  private DumpType CurrentDumpType = DumpType.ALL;

  // tells us the next dump state
  private DumpType PendingDumpType = DumpType.ALL;

  // The parts of the file names that we have recovered
  private String filePrefix;
  private String filePath;
  private String fileSuffix;
  private String ProcPrefix;

  // The writer object responsible for creating the dump file
  private BufferedWriter DumpWriter;

  // The buffer size is the size of the buffer in the buffered reader
  private static final int BUF_SIZE = 65536;

  // List of Services that this Client supports
  private final static String SERVICE_DUMPACTIVE = "DumpType";
  private final static String SERVICE_DUMPPATH   = "DumpFilePath";
  private final static String SERVICE_DUMPPREFIX = "DumpFilePrefix";
  private final static String SERVICE_DUMPSUFFIX = "DumpFileSuffix";
  private final static String SERVICE_PROCPREFIX = "ProcessingPrefix";

 /**
  * Creates a new instance of AbstractStubPlugIn. Adds the audit information
  */
  public DumpNT()
  {
    super();
  }

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
                                                           SERVICE_DUMPACTIVE,
                                                           "false");
    processControlEvent(SERVICE_DUMPACTIVE, true, ConfigHelper);

    // Synchronise the variables to the pending variables
    CurrentDumpType = PendingDumpType;

    ConfigHelper = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(PipelineName,
                                                           ModuleName,
                                                           SERVICE_DUMPPATH,
                                                           ".");
    processControlEvent(SERVICE_DUMPPATH, true, ConfigHelper);

    ConfigHelper = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(PipelineName,
                                                           ModuleName,
                                                           SERVICE_DUMPPREFIX,
                                                           "DMP");
    processControlEvent(SERVICE_DUMPPREFIX, true, ConfigHelper);

    ConfigHelper = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(PipelineName,
                                                           ModuleName,
                                                           SERVICE_DUMPSUFFIX,
                                                           ".dump");
    processControlEvent(SERVICE_DUMPSUFFIX, true, ConfigHelper);

    ConfigHelper = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(PipelineName,
                                                           ModuleName,
                                                           SERVICE_PROCPREFIX,
                                                           "tmp");
    processControlEvent(SERVICE_PROCPREFIX, true, ConfigHelper);
  }

  /**
  * This is called when the synthetic Header record is encountered, and has the
  * meaning that the stream is starting. In this case we have to open a new
  * dump file each time a stream starts.
  */
  @Override
  public IRecord procHeader(IRecord r)
  {
    HeaderRecord tmpHeader;

    // If we have a pending state change do it now
    CurrentDumpType = PendingDumpType;

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
        tmpHeader = (HeaderRecord)r;

        openDumpFile(tmpHeader.getStreamName());

        DumpInfo = r.getDumpInfo();

        // dump it
        Iterator<String> DumpIter = DumpInfo.iterator();

        while (DumpIter.hasNext())
        {

          try
          {
            DumpWriter.write(DumpIter.next());
            DumpWriter.newLine();
          }
          catch (IOException ex)
          {
            pipeLog.error("Error during processing in Module <" + getSymbolicName() + ">. Error reason <" + ex.getMessage() + ">");
          }
        }
      }
    }

    return r;
  }

  /**
  * This is called when a data record is encountered. You should do any normal
  * processing here.
  */
  @Override
  public IRecord procValidRecord(IRecord r)
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

          try
          {
            DumpWriter.write(DumpIter.next());
            DumpWriter.newLine();
          }
          catch (IOException ex)
          {
            pipeLog.error("Error during processing in Module <" + getSymbolicName() + ">. Error reason <" + ex.getMessage() + ">");
          }
        }
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

            try
            {
              DumpWriter.write(DumpIter.next());
              DumpWriter.newLine();
            }
            catch (IOException ex)
            {
              pipeLog.error("Error during processing in Module <" + getSymbolicName() + ">. Error reason <" + ex.getMessage() + ">");
            }
          }
        }
      }
    }

    return r;
  }

  /**
  * This is called when a data record with errors is encountered. You should do
  * any processing here that you have to do for error records, e.g. statistics,
  * special handling, even error correction!
  */
  @Override
  public IRecord procErrorRecord(IRecord r)
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

          try
          {
            DumpWriter.write(DumpIter.next());
            DumpWriter.newLine();
          }
          catch (IOException ex)
          {
            pipeLog.error("Error during processing in Module <" + getSymbolicName() + ">. Error reason <" + ex.getMessage() + ">");
          }
        }
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

            try
            {
              DumpWriter.write(DumpIter.next());
              DumpWriter.newLine();
            }
            catch (IOException ex)
            {
              pipeLog.error("Error during processing in Module <" + getSymbolicName() + ">. Error reason <" + ex.getMessage() + ">");
            }
          }
        }
      }
    }

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
    TrailerRecord tmpTrailer;

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
        tmpTrailer = (TrailerRecord)r;

        DumpInfo = r.getDumpInfo();

        // dump it
        Iterator<String> DumpIter = DumpInfo.iterator();

        while (DumpIter.hasNext())
        {

          try
          {
            DumpWriter.write(DumpIter.next());
            DumpWriter.newLine();
          }
          catch (IOException ex)
          {
            pipeLog.error("Error during processing in Module <" + getSymbolicName() + ">. Error reason <" + ex.getMessage() + ">");
          }
        }

        try
        {
          // Close down the dump file
          closeDumpFile();
        }
        catch (ProcessingException ex)
        {
          pipeLog.fatal("Error closing dump file");
        }

        finaliseDump(tmpTrailer.getStreamName());
      }
    }

    // If we have a pending state change do it now
    CurrentDumpType = PendingDumpType;

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

    if (Command.equalsIgnoreCase(SERVICE_DUMPACTIVE))
    {
        if (Parameter.equalsIgnoreCase("all"))
        {
          // Enable the dump at the end of the current transaction
          PendingDumpType = DumpType.ALL;
          ResultCode = 0;
        }
        else if (Parameter.equalsIgnoreCase("none"))
        {
          // Enable the dump at the end of the current transaction
          PendingDumpType = DumpType.NONE;
          ResultCode = 0;
        }
        else if (Parameter.equalsIgnoreCase("errors"))
        {
          // Enable the dump at the end of the current transaction
          PendingDumpType = DumpType.ERRORS;
          ResultCode = 0;
        }
        else if (Parameter.equalsIgnoreCase("flag"))
        {
          // Enable the dump at the end of the current transaction
          PendingDumpType = DumpType.FLAG;
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

    if (Command.equalsIgnoreCase(SERVICE_DUMPPATH))
    {
      if (Init)
      {
        if (Parameter.equals(""))
        {
          return filePath;
        }
        else
        {
          filePath = Parameter;
          ResultCode = 0;
        }
      }
      else
      {
        return CommonConfig.NON_DYNAMIC_PARAM;
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_DUMPPREFIX))
    {
      if (Init)
      {
        if (Parameter.equals(""))
        {
          return filePrefix;
        }
        else
        {
          filePrefix = Parameter;
          ResultCode = 0;
        }
      }
      else
      {
        return CommonConfig.NON_DYNAMIC_PARAM;
      }
    }
    if (Command.equalsIgnoreCase(SERVICE_DUMPSUFFIX))
    {
      if (Init)
      {
        if (Parameter.equals(""))
        {
          return fileSuffix;
        }
        else
        {
          fileSuffix = Parameter;
          ResultCode = 0;
        }
      }
      else
      {
        return CommonConfig.NON_DYNAMIC_PARAM;
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_PROCPREFIX))
    {
      if (Init)
      {
        if (Parameter.equals(""))
        {
          return ProcPrefix;
        }
        else
        {
          ProcPrefix = Parameter;
          ResultCode = 0;
        }
      }
      else
      {
        return CommonConfig.NON_DYNAMIC_PARAM;
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
  */
  @Override
  public void registerClientManager() throws InitializationException
  {
    // Set the client reference and the base services first
    super.registerClientManager();

    //Register this Client
    ClientManager.registerClient(pipeName,getSymbolicName(), this);
    
    //Register services for this Client
    ClientManager.registerClientService(getSymbolicName(), SERVICE_DUMPACTIVE, ClientManager.PARAM_MANDATORY_DYNAMIC);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_DUMPPATH, ClientManager.PARAM_NONE);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_DUMPPREFIX, ClientManager.PARAM_NONE);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_DUMPSUFFIX, ClientManager.PARAM_NONE);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_PROCPREFIX, ClientManager.PARAM_NONE);
  }

  // -----------------------------------------------------------------------------
  // --------------------- Start of custom functions -----------------------------
  // -----------------------------------------------------------------------------

  /**
  * Closes down the input stream after all the input has been collected
   *
   * @throws OpenRate.exception.ProcessingException
   */
  public void closeDumpFile()
                     throws ProcessingException
  {

    try
    {
      DumpWriter.close();
    }
    catch (IOException exFileNotFound)
    {
      pipeLog.error("Application is not able to close dump file");
      throw new ProcessingException("Application is not able to close dump file",
                                    exFileNotFound);
    }
  }

  /**
  * Open the new dump file with the name calculated from the transaction base
  * name, read by the input adapter.
   *
   * @param BaseName The base name of the transaction stream
   */
  public void openDumpFile(String BaseName)
  {

    FileWriter fwriter = null;
    File file;
    String DumpFileName;

    // Calculate the file name
    DumpFileName = filePath + System.getProperty("file.separator") +
                   ProcPrefix + filePrefix + BaseName + fileSuffix;
    file = new File(DumpFileName);

    try
    {

      if (file.createNewFile() == false)
      {
        pipeLog.error("output file already exists = " + DumpFileName);
      }

      fwriter = new FileWriter(file);
    }
    catch (IOException ex)
    {
      pipeLog.error("Error opening valid stream output for file " +
                DumpFileName);
    }

    DumpWriter = new BufferedWriter(fwriter, BUF_SIZE);
  }

  /**
   * finaliseDump closes down the processing and renames the dump file
   * to show that we have done with it.
   *
   * @param BaseName The base name of the transaction stream
   */
  public void finaliseDump(String BaseName)
  {
    String DumpFileNameOld;
    String DumpFileNameNew;

    // Calculate the old file name
    DumpFileNameOld = filePath + System.getProperty("file.separator") +
                      ProcPrefix + filePrefix + BaseName + fileSuffix;

    // Calculate the new file name
    DumpFileNameNew = filePath + System.getProperty("file.separator") +
                      filePrefix + BaseName + fileSuffix;

    // rename the input file to show that it is no longer under the TMs control
    File f = new File(DumpFileNameOld);
    f.renameTo(new File(DumpFileNameNew));
  }
}
