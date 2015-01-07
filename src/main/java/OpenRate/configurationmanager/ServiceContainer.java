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
