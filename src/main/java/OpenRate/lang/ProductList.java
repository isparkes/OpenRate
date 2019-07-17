

package OpenRate.lang;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Product list is a collection of some or all of the products associated with
 * an account.
 *
 * @author Ian
 */
public class ProductList
{
  int    ProductCount = 0;
  int    BalanceGroup = 0;
  ArrayList<CustProductInfo> ProductInstances;

 /** Creates a new instance of ProductList */
  public ProductList()
  {
    ProductInstances = new ArrayList<>();
  }

 /**
  * Add a product to the product list, passing a list of attributes. If the
  * product already exists, then we update it.
  *
  * @param ProductRefId The internal reference ID of the product
  * @param Id The ID (name) of the product to add
  * @param SubId The subscription ID of the product to add
  * @param Service The service string of the product to add
  * @param ValidFrom The start of the validity of the product to add
  * @param ValidTo The end of the validity of the product to add
  * @param Quantity The number of instances of this product
  */
  public void addProduct(long ProductRefId, String Id,String SubId,String Service,long ValidFrom,long ValidTo,int Quantity)
  {
    CustProductInfo tmpCPI;

    // We must see if we need to create or update the product
    Iterator<CustProductInfo> productIter = ProductInstances.iterator();

    // Manage possible updates in the case that we have a valid (non zero) ref id
    if (ProductRefId != 0)
    {
      while (productIter.hasNext())
      {
        tmpCPI = productIter.next();

        if (tmpCPI.ProductRefId == ProductRefId)
        {
          // This is an update
          tmpCPI.setProductID(Id);
          tmpCPI.setService(Service);
          tmpCPI.setSubID(SubId);
          tmpCPI.setUTCValidFrom(ValidFrom);
          tmpCPI.setUTCValidTo(ValidTo);
          tmpCPI.setQuantity(Quantity);

          return;
        }
      }
    }

    // Not an update, so it is an insert
    tmpCPI = new CustProductInfo();
    tmpCPI.setProductID(Id);
    tmpCPI.setService(Service);
    tmpCPI.setSubID(SubId);
    tmpCPI.setUTCValidFrom(ValidFrom);
    tmpCPI.setUTCValidTo(ValidTo);
    tmpCPI.setQuantity(Quantity);

    ProductInstances.add(tmpCPI);

    // Perform internal maintenance
    incProductCount();
  }

 /**
  * Add a product to the product list, passing a CPI instance
  *
  * @param CPIToAdd The CustomerProductInstance object to add
  */
  public void addProduct(CustProductInfo CPIToAdd)
  {
    ProductInstances.add(CPIToAdd);

    // Perform internal maintenance
    incProductCount();
  }

 /**
  * Get the balance group number
  *
  * @return The balance group ID
  */
  public int getBalanceGroup()
  {
    return BalanceGroup;
  }

 /**
  * Increment the count of the products in the list
  */
  public void incProductCount()
  {
    ProductCount++;
  }

 /**
  * Get the count of the products currently in the list
  *
  * @return The current product count
  */
  public int getProductCount()
  {
    return ProductCount;
  }

 /**
  * Set the balance group ID
  *
  * @param newBalanceGroup The new value of the balance group
  */
  public void setBalanceGroup(int newBalanceGroup)
  {
    BalanceGroup = newBalanceGroup;
  }

 /**
  * Return a given CustomerProductInstanace from the list
  *
  * @param index The index of the CPI to get
  * @return The CPI
  */
  public CustProductInfo getProduct(int index)
  {
    return ProductInstances.get(index);
  }
}
