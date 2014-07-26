/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
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

package OpenRate.lang;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class forms the basic representation of each segment of validity
 * (audit segment) for handling products with history. Each time any product
 * changes, a new audit segment is written which holds the new product set up
 * "at a glance". While this is quite wasteful in terms of space, it is
 * efficient for rating.
 *
 * @author ian
 */
public class AuditSegment
{
  // The time that this audit segment is valid from
  private long UTCSegmentValidFrom;

  // The validity of the account
  private long UTCAccountValidFrom;
  private long UTCAccountValidTo;

  // The ID of this segment
  private long AuditSegmentID = 0;

  // The product list associated with this segment
  private ProductList CustAudProds;

  // The ERAs associated with this segment
  private ConcurrentHashMap<String,String> ERAs;

  /** Creates a new instance of CustInfo */
  public AuditSegment()
  {
    CustAudProds = new ProductList();
    ERAs = new ConcurrentHashMap<>(5);
  }

 /**
  * Returns the product list for this audit segment
  *
  * @return The product list for this audit segment
  */
  public ProductList getProductList()
  {
    return CustAudProds;
  }

 /**
  * Get the ID for this audit segment
  *
  * @return The located audit segment ID
  */
  public long getAuditSegmentID()
  {
    return AuditSegmentID;
  }

 /**
  * Set the ID for this audit segment
  *
  * @param AuditSegId The audit segment ID for this audit segment
  */
  public void setAuditSegmentID(long AuditSegId)
  {
    AuditSegmentID = AuditSegId;
  }

 /**
  * Get the validity start for this audit segment
  *
  * @return The validity from date of the audit segment
  */
  public long getUTCSegmentValidFrom()
  {
    return UTCSegmentValidFrom;
  }

 /**
  * Set the validity start for this audit segment
  *
  * @param newUTCValidFrom The new valid from date
  */
  public void setUTCSegmentValidFrom(long newUTCValidFrom)
  {
    UTCSegmentValidFrom = newUTCValidFrom;
  }

 /**
  * Add an ERA to the internal ERA list associated with this Audit Segment. If
  * we get a request to put an ERA with a ID that already exists, we treat this
  * as an update. We do this on the basis that for each audit segment there
  * should only be one ERA with a given key, thus we rely on the put upsert
  * to perform this.
  *
  * @param ERAKey
  * @param ERAValue
  */
  public void putERA(String ERAKey, String ERAValue)
  {
    ERAs.put(ERAKey, ERAValue);
  }

 /**
  * Get an ERA value from the ERA list
  *
  * @param EraKey The ERA key to get
  * @return The value of the ERA key
  */
  public String getERA(String EraKey)
  {
    return ERAs.get(EraKey);
  }

 /**
  * Get all of the ERA key values from the ERA list
  *
  * @return all of the keys in the ERA structure
  */
  public ArrayList<String> getERAKeyList()
  {
    ArrayList<String> ERAKeys = new ArrayList<>();

    ERAKeys.addAll(ERAs.keySet());

    return ERAKeys;
  }

 /**
  * Return the audit segment product list
  *
  * @return the products on the current audit segment
  */
  public ProductList getCustAudProds()
  {
    return CustAudProds;
  }

  /**
   * Get the Extended Rating Attrributes
   *
   * @return The ERAs
   */
  public ConcurrentHashMap<String, String> getERAs()
  {
    return ERAs;
  }

  /**
   * Get the account valid from date
   *
   * @return the UTCAccountValidFrom
   */
  public long getUTCAccountValidFrom()
  {
    return UTCAccountValidFrom;
  }

  /**
   * Set the account valid from date
   *
   * @param UTCAccountValidFrom the UTCAccountValidFrom to set
   */
  public void setUTCAccountValidFrom(long UTCAccountValidFrom)
  {
    this.UTCAccountValidFrom = UTCAccountValidFrom;
  }

  /**
   * Get the account valid to date
   *
   * @return the UTCAccountValidTo
   */
  public long getUTCAccountValidTo()
  {
    return UTCAccountValidTo;
  }

  /**
   * Set the account valid to date
   *
   * @param UTCAccountValidTo the UTCAccountValidTo to set
   */
  public void setUTCAccountValidTo(long UTCAccountValidTo)
  {
    this.UTCAccountValidTo = UTCAccountValidTo;
  }
}
