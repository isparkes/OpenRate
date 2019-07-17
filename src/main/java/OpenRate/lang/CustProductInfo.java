

package OpenRate.lang;

/**
 * Class to encapsulate the customer/product information
 *
 * @author Ian
 */
public class CustProductInfo
{
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
    ProductID = Id.intern();
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
    SubId = Id.intern();
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
    Service = newService.intern();
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
