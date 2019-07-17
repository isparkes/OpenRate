
package OpenRate.configurationmanager;

import OpenRate.OpenRate;
import OpenRate.exception.InitializationException;
import OpenRate.resource.IResource;
import OpenRate.utils.PropertyUtils;
import java.io.*;
import java.util.Calendar;

/**
 * The EventHandler class acts as the central configuration controller and
 * event dispatcher for the interface between the External Control Interface
 * (ECI) and the modules.
 *
 * On framework boot the modules are loaded and are registered (if they publish
 * IEventInterface), which causes all of the configuration properties of the
 * modules to be known, as well as the mandatory status of each module.
 *
 * After this, the EventHandler is triggered to check the configuration for the
 * module, and if everything is OK, push the configuration to the module.
 *
 * @author a.villena
 * @author i.sparkes
 */
public class EventHandler implements IResource
{
  // This is the symbolic name of the resource
  private String SymbolicName;

  /**
   * This is the key name we will use for referencing this object from the
   * Resource context
   */
  public static final String RESOURCE_KEY = "ECI";

  // The socket used for communication
  private OpenRateSocket openRateSoc;
  
  // The thread the connection listener runs in
  Thread socketThread;

  // The full path of the semaphore file
  private String semaphoreFileLocation;
  private File semaphoreFile;

 /**
  * Creates a new instance of EventHandler
  */
  public EventHandler()
  {
    super();
  }

  @Override
  public void init(String ResourceName) throws InitializationException
  {
    String ConfigHelper;

    // set up the listener, which runs in a seaprate thread
    System.out.println("    Initialising listener...");

    if (!ResourceName.equalsIgnoreCase(RESOURCE_KEY))
    {
      // we are relying on this name to be able to find the resource
      // later, so stop if it is not right
      throw new InitializationException("ECI ModuleName should be <" + RESOURCE_KEY + ">",getSymbolicName());
    }

    // Set the symbolic name
    SymbolicName = ResourceName;

    // See if we have a file interface for this listener
    ConfigHelper = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"SemaphoreFile","None");

    if (ConfigHelper.equalsIgnoreCase("None") == false)
    {
      // configure the semaphore file
      semaphoreFile = new File(ConfigHelper);
      semaphoreFileLocation = ConfigHelper;

      OpenRate.getOpenRateFrameworkLog().info("Using Semaphore File <" + semaphoreFile.getAbsolutePath() + ">");
      System.out.println("    Using Semaphore File <" + semaphoreFile.getAbsolutePath() + ">");

      // Check that the file is writeable
      File semaphoreTestFile = new File(semaphoreFileLocation+"_Test"+Calendar.getInstance().getTimeInMillis());
      try
      {
        if (semaphoreTestFile.createNewFile()) {
          semaphoreTestFile.delete();
        } else {
          // we are relying on this name to be able to find the resource
          // later, so stop if it is not right
          throw new InitializationException("Semaphore File <" + semaphoreFileLocation + "> is not a valid path",getSymbolicName());
        }
      } catch (IOException ex) {
        throw new InitializationException("Semaphore File <" + semaphoreFileLocation + "> is not a valid path",ex,getSymbolicName());
      }

      // Move any existing semaphores out of the way
      if (semaphoreFile.exists())
      {
        String newSemaphoreFileName = semaphoreFileLocation + "_STARTUP_" + Calendar.getInstance().getTimeInMillis();
        File newSemaphoreFile = new File(newSemaphoreFileName);
        semaphoreFile.renameTo(newSemaphoreFile);
        OpenRate.getOpenRateFrameworkLog().debug("renamed semaphore file <" + semaphoreFile.getAbsolutePath() + "> to <" + newSemaphoreFile.getAbsolutePath() + ">");
      }
    }

    // Open the socket listener
    openRateSoc = new OpenRateSocket(ResourceName);
    socketThread = new Thread(openRateSoc, "OpenRate ECI Listener");
    socketThread.start();
  }

  /**
   * Find out whether the socket has been started
   *
   * @return true if it has been started
   */
  public boolean getSocketStarted()
  {
    return openRateSoc.getStarted();
  }

 /**
  * registerModule gathers the configuration for the module and stores
  * this in the client manager data store
  *
  * @param objClient Object reference of the module to register
  * @param SymbolicName Symbolic name of the module to register
  * @throws OpenRate.exception.InitializationException
  */
  public void registerModule(Object objClient, String SymbolicName)
    throws InitializationException
  {
    ((IEventInterface)objClient).registerClientManager();
  }

  @Override
  public void close()
  {
    //close the socket and release
    openRateSoc.stop();
    
    // wait for listeners to stop
    while (getSocketStarted() == true)
    {
      try {
        OpenRate.getOpenRateFrameworkLog().debug("Sleeping 100mS for listener to stop");
        Thread.sleep(100);
      } catch (InterruptedException ex) {
      }
    }
  }

 /**
  * Return the resource symbolic name
  *
  * @return Symbolic name
  */
  @Override
  public String getSymbolicName()
  {
    return SymbolicName;
  }

  /**
   * looks for a semaphore file, and in the case that one is found, processes it
   */
  public void processSemaphoreFile()
  {
    if (semaphoreFile != null && semaphoreFile.exists())
    {
      // Rename the file
      String newSemaphoreFileName = semaphoreFileLocation + "_" + Calendar.getInstance().getTimeInMillis();
      File newSemaphoreFile = new File(newSemaphoreFileName);
      semaphoreFile.renameTo(newSemaphoreFile);
      OpenRate.getOpenRateFrameworkLog().debug("renamed semaphore file <" + semaphoreFile.getAbsolutePath() + "> to <" + newSemaphoreFile.getAbsolutePath() + ">");

      try
      {
        try (BufferedReader inputFile = new BufferedReader(new FileReader(newSemaphoreFile))) {
          String semaphoreRequest;
          String semaphoreResponse;

          while ((semaphoreRequest = inputFile.readLine()) != null)
          {
            OpenRate.getOpenRateFrameworkLog().info("Processing Semaphore <" + semaphoreRequest + ">");
            semaphoreResponse = processSemaphoreInput(semaphoreRequest);
            OpenRate.getOpenRateFrameworkLog().info("Semaphore Result <" + semaphoreResponse + ">");
          }
        }
      }
      catch (FileNotFoundException ex)
      {
        OpenRate.getOpenRateFrameworkLog().info("Error reading semaphore file <" + semaphoreFile.getAbsolutePath() + ">");
      }
      catch (IOException ex)
      {
        OpenRate.getOpenRateFrameworkLog().info("Error accessing semaphore file <" + semaphoreFile.getAbsolutePath() + ">");
      }
    }
  }

  /**
   * Processes a single line from a semaphore file, returning the output
   *
   * @param inputLine The line from the semaphore file to process
   * @return The result of the semaphore
   */
  public String processSemaphoreInput(String inputLine)
  {
    //parse command string by splitting on the ":" and "=" notation
    String strInputParams[] = inputLine.split(":");
    String CmdParameter;
    String CmdCommand;
    Object ClientObject;
    IEventInterface ClientEvent;
    String output = "";

    if (strInputParams.length == 2)
    {
      // Command appears to be in the correct form of notation, try to process
      // it further.  It is assumed that the form of command is
      // "Client:Service=Args" where Service=Args is the specific service to
      // be executed with a single argument
      // This is the module symbolic name that we are dealing with
      String CmdModuleSymbolicName = strInputParams[0];

      // See if there are any arguments
      String CommandParams[] = strInputParams[1].split("=");

      if (CommandParams.length == 1)
      {
        CmdParameter = "";
        CmdCommand = strInputParams[1];
      }
      else
      {
        CmdCommand = CommandParams[0];
        CmdParameter = CommandParams[1];
      }
      ClientContainer clCon = ClientManager.getClientManager().get(CmdModuleSymbolicName);

      if (clCon != null)
      {
        //there is a cached ClientObject; process it

        ClientObject  = clCon.getClientObject();
        if (ClientObject instanceof IEventInterface)
        {
          ClientEvent = (IEventInterface) ClientObject;

          //call ProcesControlEvent on second parameter of input
          output = ClientEvent.processControlEvent(CmdCommand,false,CmdParameter);
        }
        else
        {
          OpenRate.getOpenRateFrameworkLog().error("Module <" + CmdModuleSymbolicName + "> is not able to respond to the event interface");
        }
      }
    }

    return output;
  }
}
