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
