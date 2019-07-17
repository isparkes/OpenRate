

package OpenRate.configurationmanager;

/**
 * ServiceContainer defines the container object that holds the service/command properties of
 * a client module.
 *
 * @author ian
 */
public class ServiceContainer
{
  private String  Name;
  private boolean Mandatory;
  private boolean Dynamic;
  private boolean Loaded = false;
  private boolean RequireSync;

 /**
  * Creates a new instance of ServiceContainer
  *
  * @param NewName The name of the service container
  * @param Mand If the service is mandatory
  * @param Dyn If the service is dynamic
  * @param ReqSync If the service requries a sync point
  */
  public ServiceContainer(String NewName, boolean Mand, boolean Dyn, boolean ReqSync)
  {
    Name = NewName;
    Mandatory = Mand;
    Dynamic = Dyn;
    RequireSync = ReqSync;
  }

 /**
  * getMandatory is a getter method to check if a command is mandatory or not
  *
  * @return boolean - true if mandatory, false if not
  */
  public boolean getMandatory()
  {
    return Mandatory;
  }

 /**
  * getDynamic is a getter method to check if a command is dynamic or not
  *
  * @return boolean - true if dynamic, false if not
  */
  public boolean getDynamic()
  {
    return Dynamic;
  }

 /**
  * setDynamic is a setter method to set the command to dynamic/not dynamic
  *
  * @param isDynamic - set the command if dynamic (true) or not (false)
  */
  public void setDynamic(boolean isDynamic)
  {
    Dynamic = isDynamic;
  }

 /**
  * setLoaded is a setter method to mark the fact that the item has been
  * configured
  */
  public void setLoaded()
  {
    Loaded = true;
  }

 /**
  * getLoaded is a getter method to check if a command is mandatory, and that
  * it has been loaded
  *
  * @return boolean - true if mandatory and loaded, or not mandatory,
  *                   otherwise false. Any false return code therefore
  *                   indicates an error.
  */
  public boolean getLoaded()
  {
    if ( this.Mandatory )
    {
      return this.Mandatory & this.Loaded;
    }
    else
    {
      return true;
    }
  }

 /**
  * setRequireSync is a setter method to mark the fact that the item needs a
  * sync point before being processed
  *
  * @param newRequireSync True if requires sync, otherwise false
  */
  public void setRequireSync(boolean newRequireSync)
  {
    RequireSync = newRequireSync;
  }

 /**
  * getRequireSync is a getter method to check if a command is requires that the
  * event performs a sync before processing
  *
  * @return boolean - true if requires sync, otherwise false
  */
  public boolean getRequireSync()
  {
    return RequireSync;
  }
}
