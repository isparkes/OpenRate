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

package OpenRate.lang;

/**
 * Class to encapsulate the customer/product information
 *
 * @author Ian
 */
public class CustProductInfo
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: CustProductInfo.java,v $, $Revision: 1.16 $, $Date: 2013-05-13 18:12:11 $";

  private static final long HIGH_DATE = 2147483647;

 /**
  * The CustProductInfo structure holds the information about the products the,
  * customer has, including the validity dates. Note that we are using long integers
  * for the dates to reduce storage requirements.
  */
  String ProductID=null;
  String Service=null;
  String SubId=null;
  long   ProductRefId;
  long   UTCValidFrom = 0;
  long   UTCValidTo = HIGH_DATE;
  int    Quantity = 0;
  int    Status = 0;
  int    Priority = 0;

  /**
   * Creates a new instance of CustProductInfo
   */
  public CustProductInfo()
  {
    // Nothing
  }

  /**
   * Sets the product ID
   *
   * @param Id The new product ID to set
   */
  public void setProductID(String Id)
  {
    ProductID = Id;
  }

  /**
   * Gets the product ID
   *
   * @return The product ID
   */
  public String getProductID()
  {
    return ProductID;
  }

  /**
   * Sets the subscription ID
   *
   * @param Id The subscription ID
   */
  public void setSubID(String Id)
  {
    SubId = Id;
  }

  /**
   * Sets the subscription ID
   *
   * @return The subscription ID
   */
  public String getSubID()
  {
    return SubId;
  }

  /**
   *
   * @param newService
   */
  public void setService(String newService)
  {
    Service = newService;
  }

  /**
   *
   * @return
   */
  public String getService()
  {
    return Service;
  }

  /**
   * Sets the valid from date
   *
   * @param ValidFrom The valid from date
   */
  public void setUTCValidFrom(long ValidFrom)
  {
    UTCValidFrom = ValidFrom;
  }

  /**
   * Sets the valid to date
   *
   * @param ValidTo The valid to date
   */
  public void setUTCValidTo(long ValidTo)
  {
    UTCValidTo = ValidTo;
  }

  /**
   * Sets the quantity
   *
   * @param NewQuantity The new quantity
   */
  public void setQuantity(int NewQuantity)
  {
    Quantity = NewQuantity;
  }

  /**
   * Gets the quantity
   *
   * @return The quantity
   */
  public int getQuantity()
  {
    return Quantity;
  }

  /**
   * Gets the valid from date
   *
   * @return The valid from date
   */
  public long getUTCValidFrom()
  {
    return UTCValidFrom;
  }

  /**
   * Gets the valid to date
   *
   * @return The valid to date
   */
  public long getUTCValidTo()
  {
    return UTCValidTo;
  }

  /**
   * Sets the new status
   *
   * @param newStatus The new status
   */
  public void setStatus(int newStatus)
  {
    Status = newStatus;
  }

  /**
   * Gets the status
   *
   * @return The status
   */
  public int getStatus()
  {
    return Status;
  }

  /**
   * Sets the priority
   *
   * @param newPriority The priority
   */
  public void setPriority(int newPriority)
  {
    Priority = newPriority;
  }

  /**
   * Gets the priority
   *
   * @return The priority
   */
  public int getPriority()
  {
    return Priority;
  }

  /**
   * Sets the product reference ID
   *
   * @param newProductRefId The product reference id
   */
  public void setProductRefId(int newProductRefId)
  {
    ProductRefId = newProductRefId;
  }

  /**
   * Gets the product reference ID
   *
   * @return The product reference ID
   */
  public long getProductRefId()
  {
    return ProductRefId;
  }
}
