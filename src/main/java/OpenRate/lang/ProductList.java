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
