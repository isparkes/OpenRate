/* ===================================================================
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

package OpenRate.cache;

import OpenRate.CommonConfig;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.lang.AuditSegment;
import OpenRate.lang.CustInfo;
import OpenRate.lang.CustProductInfo;
import OpenRate.lang.ProductList;
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
import OpenRate.utils.PropertyUtils;
import java.io.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements a cache of customer information for use in
 * rating based on product instances, reading the data from a Portal database.
 * This uses the audit tables, so that changes to the product list are fully
 * historicised.
 *
 * The loading process goes like this:
 *  1) Get a list of the aliases and the associated service ids. This means that
 *     we are able to deal with aliases more easily later
 *  2) Get a list of all the audit segements (history segments) for each service
 *  3) For each segment, get a list of the products, with validities
 *
 * ------------------------------ File Interface -------------------------------
 *
 * The verbs in the file are:
 *
 *     01 = Add Customer Account Audit Segments
 *     02 = Add Customer Product
 *     05 = Add Alias
 *     06 = Add ERA
 *
 * Verb descriptions:
 *
 *     01, Add Customer Account Audit Segment
 *     Record format:
 *       01;AuditSegID;CustID;ExternalCustId;AudSegDate;ValidFrom;ValidTo
 *
 *     02, Add Customer Product
 *     Record format:
 *       02;AuditSegID;ProductIdentifier;SubscriptionIdentifier;Service;ValidFrom;ValidTo
 *
 *     05, Add Alias
 *     Record format:
 *       05;Alias;CustomerIdentifier,SubscriptionIdentifier,AliasValidFrom,AliasValidTo
 *
 *     06, Add ERA
 *     Record format:
 *       06;AuditSegID;ERAKey;ERAValue
 *
 * The verbs presented here do not allow dynamic update, and in order to do this
 * additional verbs will have to be created.
 *
 * ------------------------------- DB Interface --------------------------------
 *
 * If the loading is done from a DB, the queries that are executed are:
 *
 * AliasSelectQuery: This query should return a list of the aliases that are
 * to be associated with each account, and the subscription that they are
 * associated with on the account. The query should return:
 *
 * 1) ALIAS_IDENTIFIER (used for incremental update)
 * 2) ALIAS
 * 3) CUSTOMER_IDENTIFIER
 * 4) SUBSCRIPTION_IDENTIFIER
 * 5) VALID_FROM (YYYYMMDDHHMMSS)
 * 6) VALID_TO (YYYYMMDDHHMMSS)
 * 7) ModT (YYYYMMDDHHMMSS)
 *
 * AuditSegmentSelectStatement: This query returns a list of all of the customer
 * accounts that the system should handle. The query should return:
 *
 * 1) AUDIT_SEGMENT_IDENTIFIER (used for incremental update)
 * 2) CUSTOMER_IDENTIFIER
 * 3) ExternalCustId
 * 4) AUDIT_SEGMENT_VALID_FROM (YYYYMMDDHHMMSS)
 * 5) VALID_FROM (YYYYMMDDHHMMSS)
 * 6) VALID_TO (YYYYMMDDHHMMSS)
 * 7) ModT (YYYYMMDDHHMMSS)
 *
 * ProductSelectQuery: This query returns a list of all the products that are
 * associated with a customer account. The query should return:
 *
 * 1) PRODUCT_ID (used for incremental update)
 * 2) AUDIT_SEGMENT_IDENTIFIER
 * 3) PRODUCT_NAME
 * 4) SUBSCRIPTION_IDENTIFIER
 * 5) SERVICE
 * 6) VALID_FROM (YYYYMMDDHHMMSS)
 * 7) VALID_TO (YYYYMMDDHHMMSS)
 * 8) ModT (YYYYMMDDHHMMSS)
 *
 * ERASelectQuery: This query returns a list of all the ERA that are associated
 * with a customer account. The query should return:
 *
 * 1) ERA_ID
 * 2) AUDIT_SEGMENT_IDENTIFIER
 * 3) ERA_KEY
 * 4) ERA_VALUE
 * 5) ModT (YYYYMMDDHHMMSS)
 *
 * ModT is used to track changes and merge them into the existing cache. Each
 * time a change is made, ModT in the DB is updated, and we use this fact
 * to gather the changes and either add new data or update existing data.
 *
 * Generally we know to update by the fact that we have a new ModT for an
 * existing AuditSegID.
 *
 * @author i.sparkes
 */
public class CustomerCacheAudited
    extends AbstractSyncLoaderCache
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: CustomerCacheAudited.java,v $, $Revision: 1.67 $, $Date: 2013-05-13 18:12:10 $";

  // Get access to the FWlog
  protected ILogger FWlog = LogUtil.getLogUtil().getLogger("Framework");

  // this is the persistent result set that we use to incrementally get the records
  protected ResultSet crs = null;
  protected ResultSet ars = null;
  protected ResultSet prs = null;
  protected ResultSet ers = null;

  // these are the statements that we have to prepare to be able to get
  // records once and only once
  protected String aliasSelectQuery;
  protected String auditSelectQuery;
  protected String productSelectQuery;
  protected String eraSelectQuery;

  // these are the prepared statements
  protected PreparedStatement stmtAliasSelectQuery;
  protected PreparedStatement stmtAuditSelectQuery;
  protected PreparedStatement stmtProductSelectQuery;
  protected PreparedStatement stmtERASelectQuery;

 /**
  * Used to allow alias maps - go in with the alias, and it will return the
  * unique customer AuditSegID for you
  */
  protected ConcurrentHashMap<String,ValidityNode> aliasCache;

 /**
  * This stores the history segments of the product information. Go in with the
  * customer AuditSegID and it returns all the versions of the information for that cust.
  */
  protected ConcurrentHashMap<Integer, CustInfo> custCache;

 /**
  * This stores the customer history segment to the products. Go in with the history
  * segment and it returns the product list for that segment.
  */
  protected ConcurrentHashMap<Long, AuditSegment> auditSegmentCache;

 /**
  * The internal date format is the format that by default will be used when
  * interpreting dates that come in the queries. The value here is the default
  * value, but this can be changed.
  */
  protected String internalDateFormat = "yyyyMMddHHmmss";

  // List of Services that this Client supports
  protected final static String SERVICE_UPDATE_FREQUENCY = "UpdateFrequency";
  protected final static String SERVICE_DUMP_INFO = "DumpInfo";

  // this is the update frequency that will determine how often the
  // cache information is updated from the DB
  protected int updateFrequency = 300;

  // this is the last time that we performed an update
  protected long lastUpdate;

  // These are the ModT values that we use to perform incremental updates
  protected long lastAccountVerModT = 0;
  protected long lastAliasModT = 0;
  protected long lastERAModT = 0;
  protected long lastProductModT = 0;

  /**
   * A validityNode is a segment of validity of a resource. These are chained
   * together in a sorted linked list. The sorting is done at insertion time
   * into the list, meaning that lookups at run time can be optimised.
   */
  protected class ValidityNode
  {
    long         ID;         // Required for managing updates
    long         validFrom;
    long         validTo;
    Integer      custId = 0;
    String       subId = "";
    ValidityNode child = null;
  }

 /**
  * Constructor
  * Creates a new instance of the Customer Cache. The Cache
  * contains all of the Customer IDs that have been cached.
  */
  public CustomerCacheAudited()
  {
    super();

    aliasCache        = new ConcurrentHashMap<>(5000);
    custCache         = new ConcurrentHashMap<>    (5000);
    auditSegmentCache = new ConcurrentHashMap<>(5000);
  }

  // -----------------------------------------------------------------------------
  // ------------------ Start of loading internal functions ----------------------
  // -----------------------------------------------------------------------------
 /**
  * Add a alias into the cache. If the alias already exists, then we
  * update the cache information.
  *
  * @param ID The ID of the alias, used for managing updates to an existing row
  * @param alias The alias alias we want to store
  * @param custId The customer AuditSegID that the alias refers to
  * @param subID The subscription AuditSegID for this alias
  * @param validFrom The start of the validity for this alias
  * @param validTo The end of the validity for this alias
  */
  public void addAlias(long ID, String alias, Integer custId, String subID, long validFrom, long validTo)
  {
    ValidityNode newNode;
    ValidityNode tmpValidityNode;
    ValidityNode tmpValidityNextNode;
    long         lastValidTo;
    long         tmpTimeFrom;

    // Check that the valid to is after the valid from
    if (validFrom > validTo)
    {
      // Otherwise write an error and ignore it
      FWlog.error("Alias ID <" + alias + "> validity period from <" + validFrom + "> is after validity period to <" + validTo + ">. Ignoring.");

      return;
    }

    // Now add the validity segment into the list
    if (!aliasCache.containsKey(alias))
    {
      // We do not know this alias - Create the new list
      tmpValidityNode = new ValidityNode();
      tmpValidityNode.ID = ID;
      tmpValidityNode.validFrom = validFrom;
      tmpValidityNode.validTo = validTo;
      tmpValidityNode.custId = custId;
      tmpValidityNode.subId = subID;
      tmpValidityNode.child = null;

      // Add in the new node
      aliasCache.put(alias, tmpValidityNode);

      // Done
      return;
    }
    else
    {
      // Recover the validity map that there is
      tmpValidityNode = aliasCache.get(alias);

      // preset our valid to date
      lastValidTo = CommonConfig.LOW_DATE;

      // now run down the validity periods until we find the right position
      while (tmpValidityNode != null)
      {
        tmpValidityNextNode = tmpValidityNode.child;

        if (tmpValidityNextNode != null)
        {
          tmpTimeFrom = tmpValidityNextNode.validFrom;
        }
        else
        {
          tmpTimeFrom = CommonConfig.HIGH_DATE;
        }

        // Check to see if we have a simple update
        if ((tmpValidityNode.ID == ID) && (ID != 0))
        {
          // This is a simple update
          tmpValidityNode.validFrom = validFrom;
          tmpValidityNode.validTo = validTo;
          tmpValidityNode.custId = custId;
          tmpValidityNode.subId = subID;

          // done
          return;
        }

        if ((validFrom > tmpValidityNode.validTo) &
            (tmpValidityNextNode == null))
        {
          // insert at the tail of the list if we are able
          newNode = new ValidityNode();
          tmpValidityNode.child = newNode;
          newNode.ID = ID;
          newNode.validFrom = validFrom;
          newNode.validTo = validTo;
          newNode.custId = custId;
          newNode.subId = subID;

          // done
          return;
        }
        else if ((validFrom > lastValidTo) &
            (validTo <= tmpValidityNode.validFrom))
        {
          // insert at the head of the list
          newNode = new ValidityNode();

          // move the information over
          newNode.ID = tmpValidityNode.ID;
          newNode.validFrom = tmpValidityNode.validFrom;
          newNode.validTo = tmpValidityNode.validTo;
          newNode.subId = tmpValidityNode.subId;
          newNode.custId = tmpValidityNode.custId;
          newNode.child = tmpValidityNode.child;

          // add the new information
          tmpValidityNode.ID = ID;
          tmpValidityNode.validFrom = validFrom;
          tmpValidityNode.validTo = validTo;
          tmpValidityNode.subId = subID;
          tmpValidityNode.custId = custId;
          tmpValidityNode.child = newNode;

          // done
          return;
        }
        else if ((validFrom > tmpValidityNode.validTo) & (validTo <= tmpTimeFrom))
        {
          // insert in the middle of the list
          newNode = new ValidityNode();
          newNode.child = tmpValidityNode.child;
          tmpValidityNode.child = newNode;
          newNode.ID = ID;
          newNode.validFrom = validFrom;
          newNode.validTo = validTo;
          newNode.custId = custId;
          newNode.subId = subID;

          // done
          return;
        }

        // Move down the map
        lastValidTo = tmpValidityNode.validTo;
        tmpValidityNode = tmpValidityNode.child;
      }
    }

    // if we get here, we could not insert correctly
    String Message = "Alias ID <" + alias + "> already exists for time <" + validFrom + "-" + validTo + ">";
    FWlog.error(Message);
  }

 /**
  * Add an audit segment into the CustomerCache.
  * The identifier here of the customer is the internal key
  *
  * @param custId The internal customer account identifier
  * @param balanceGroup The AuditSegID of the balance group to use for this account
  * @param ExtCustID The external customer ID
  * @param auditSegId The AuditSegID of the audit segment
  * @param audSegValidFrom The start of the validity of this audit segment
  * @param custValidFrom The start of the validity of the customer for this audit segment
  * @param custValidTo The end of the validity of the customer for this audit segment
  * @throws InitializationException
  */
  public void addAuditSegment(long    auditSegId,
                              Integer custId,
                              String  ExtCustID,
                              long    balanceGroup,
                              long    audSegValidFrom,
                              long    custValidFrom,
                              long    custValidTo) throws InitializationException
  {
    CustInfo tmpCustInfo;
    AuditSegment tmpAuditSegment;

    // See if we already have AuditSegID for this customer
    if (!custCache.containsKey(custId))
    {
      // If not, create the new entry for the customer AuditSegID
      tmpCustInfo = new CustInfo();
      tmpCustInfo.ExternalCustId = ExtCustID;
      tmpCustInfo.balanceGroup = balanceGroup;
      custCache.put(custId,tmpCustInfo);
    }
    else
    {
      // Otherwise just recover it
      tmpCustInfo = custCache.get(custId);
    }

    // See if we are dealing with an update - we do this by searching for the AuditSegID
    tmpAuditSegment = tmpCustInfo.getAuditSegmentByID(auditSegId);

    if (tmpAuditSegment != null)
    {
      // We are doing an update - update the validity dates
      tmpAuditSegment.setUTCSegmentValidFrom(audSegValidFrom);
      tmpAuditSegment.setUTCAccountValidFrom(custValidFrom);
      tmpAuditSegment.setUTCAccountValidTo(custValidTo);

      // done - get out
      return;
    }

    // Creating a new audit segment - will return null if it fails
    tmpAuditSegment = tmpCustInfo.createAuditSegment(audSegValidFrom);

    if (tmpAuditSegment == null)
    {
      // Null response means that we were not able to cover the period
      // without overlaps
      String Message = "Attempting to add an audit segment to custId <" + custId + "> with start date <" + audSegValidFrom + ">, but this already exists";
      FWlog.error(Message);
    }
    else
    {
      // Set the audit segment information
      tmpAuditSegment.setAuditSegmentID(auditSegId);
      tmpAuditSegment.setUTCAccountValidFrom(custValidFrom);
      tmpAuditSegment.setUTCAccountValidTo(custValidTo);
      
      // put the information into the contruction cache
      auditSegmentCache.put(auditSegId,tmpAuditSegment);
    }
  }

 /**
  * Add a CPI value into the CustomerCache
  *
  * The identifier here of the customer is the audit segment AuditSegID
  *
  * @param auditSegId The audit segment AuditSegID
  * @param productRefId The reference value for this row, used to manage updates
  * @param prodID The product name
  * @param subId The subscription AuditSegID
  * @param service The service
  * @param prodValidFrom The start of the validity date of the product
  * @param prodValidTo The end of the validity date of the product
  * @throws InitializationException
  */
  public void addAuditedCPI(long auditSegId, long productRefId, String prodID, String subId, String service,long prodValidFrom, long prodValidTo) throws InitializationException
  {
    AuditSegment tmpAuditSegment;

    // Recover the audit segment
    tmpAuditSegment = auditSegmentCache.get(auditSegId);

    if (tmpAuditSegment == null)
    {
      String Message = "Attempting to add a product <" + productRefId + "> to a non-existent audit segment <" + auditSegId + ">";
      getFWLog().error(Message);
    }
    else
    {
      // adjust the to high date
      if (prodValidTo == 0)
      {
        prodValidTo = CommonConfig.HIGH_DATE;
      }

      // Add the product to the audit segment
      tmpAuditSegment.getProductList().addProduct(productRefId,prodID,subId,service,prodValidFrom,prodValidTo,1);
    }
  }

 /**
  * Add an ERA value into the CustomerCache
  *
  * @param AuditSegId The audit segment AuditSegID to add the ERA to
  * @param ERAKey The ERA key to add
  * @param ERAValue The ERA Value to add
  * @throws InitializationException
  */
  public void addAuditedERA(long AuditSegId, String ERAKey, String ERAValue)
          throws InitializationException
  {
    AuditSegment tmpAuditSegment;

    // Recover the audit segment
    tmpAuditSegment = auditSegmentCache.get(AuditSegId);

    if (tmpAuditSegment == null)
    {
      String Message = "Attempting to add an ERA <" + ERAKey + "> to a non-existent audit segment <" + AuditSegId + ">";
      getFWLog().error(Message);
    }
    else
    {
      // The put method manages the update or insert automatically
      tmpAuditSegment.putERA(ERAKey,ERAValue);
    }
  }

  // -----------------------------------------------------------------------------
  // ---------------------- Start of retrieval functions -------------------------
  // -----------------------------------------------------------------------------

 /**
  * Get a list of all products that are available at the time of the CDR, for a
  * given Alias. Note that this takes into account the subId of the alias to
  * limit the number of products returned. It does not however limit on the
  * basis of the service.
  *
  * @param alias The alias to recover the product list for
  * @param cdrDate The date to recover the product list for
  * @return The products for the account and date
  * @throws ProcessingException
  */
  public ProductList getProducts(String alias, long cdrDate) throws ProcessingException
  {
    Integer custId = null;
    ValidityNode tmpValidityNode;
    String subId = null;

    // See if we already have AuditSegID for this customer
    if (aliasCache.containsKey(alias))
    {
      // get the start of the search tree
      tmpValidityNode = aliasCache.get(alias);

      // Now that we have the Validity Map, get the entry
      while (tmpValidityNode != null)
      {
        if ((tmpValidityNode.validFrom <= cdrDate) &
            (tmpValidityNode.validTo > cdrDate))
        {
          custId = tmpValidityNode.custId;
          subId = tmpValidityNode.subId;
          break;
        }

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }

      if (custId == null)
      {
        // Otherwise write an error and ignore it
        String Message = "Alias <" + alias + "> not found for time <" + cdrDate + ">. Lookup failed.";
        throw new ProcessingException(Message);
      }
      else
      {
        // recover the products with the Cust ID
        return getProducts(custId,subId,cdrDate);
      }
    }
    else
    {
      // Otherwise write an error and ignore it
      String Message = "Alias <" + alias + "> not found. Lookup failed.";
      throw new ProcessingException(Message);
    }
  }

 /**
  * Get a list of all products that are available at the time of the CDR, for a
  * given Alias. Note that this takes into account the subId of the alias to
  * limit the number of products returned. It does not however limit on the
  * basis of the service.
  *
  * @param CustId The customer ID o recover the products for
  * @param SubscriptionID The subscription ID to recover for (null = all)
  * @param CDRDate The date to recover the product list for
  * @return The products for the account and date
  * @throws ProcessingException
  */
  public ProductList getProducts(Integer CustId, String SubscriptionID, long CDRDate) throws ProcessingException
  {
    CustInfo tmpCustInfo;
    ProductList resultProductList;
    AuditSegment tmpAuditSegment;

    // Prepare the result
    resultProductList = new ProductList();

    // get the customer for the alias
    tmpCustInfo = custCache.get(CustId);

    //get the correct audit segment
    tmpAuditSegment = tmpCustInfo.getBestAuditSegmentMatch(CDRDate);

    if (tmpAuditSegment != null)
    {
      FWlog.debug("Using audit segment <" + tmpAuditSegment.getAuditSegmentID() + "> for Cust ID <" + CustId + "> at time <" + CDRDate + ">");

      // Check the validity of the customer account
      if ((tmpAuditSegment.getUTCAccountValidFrom() <= CDRDate) && (tmpAuditSegment.getUTCAccountValidTo() > CDRDate))
      {
        // the account is valid - return the list
        return getProducts(tmpAuditSegment, SubscriptionID);
      }
      else
      {
        // The account is not valid at this time
        String Message = "Account id <" + CustId + "> not valid at time <" + CDRDate + "> in audit segment <" + tmpAuditSegment.getAuditSegmentID() + ">";
        throw new ProcessingException(Message);
      }
    }

    return resultProductList;
  }

 /**
  * Get a list of all the products from an audit segment, matching the
  * subId.
  *
  * @param tmpAuditSegment The audit setment to recover the products from
  * @param subId The subscription AuditSegID to recover the products for
  * @return The products from the audit segment for the given sub AuditSegID
  */
  public ProductList getProducts(AuditSegment tmpAuditSegment, String subId)
  {
    ProductList tmpProductList;
    ProductList resultProductList;
    int i;

    // Prepare the result
    resultProductList = new ProductList();

    // Get the product list
    tmpProductList = tmpAuditSegment.getProductList();

    // Now remove the products that do not match the audit segment
    if (subId == null)
    {
      // return all products
      resultProductList = tmpProductList;
    }
    else
    {
      // get only the products that match the subscription
      for (i = 0 ; i < tmpProductList.getProductCount() ; i++ )
      {
        if (tmpProductList.getProduct(i).getSubID().equals(subId))
        {
          resultProductList.addProduct(tmpProductList.getProduct(i));
        }
      }
    }

    return resultProductList;
  }

 /**
  * Gets an internal custID for a given alias and date
  *
  * @param alias The alias for the customer account
  * @param cdrDate The date to recover the internal cust AuditSegID for
  * @return The internal customer AuditSegID
  */
  public Integer getCustId(String alias, long cdrDate)
  {
    Integer custId = null;
    ValidityNode tmpValidityNode;

    // See if we already have AuditSegID for this customer
    if (alias!= null && aliasCache.containsKey(alias))
    {
      // get the start of the search tree
      tmpValidityNode = aliasCache.get(alias);

      // Now that we have the Validity Map, get the entry
      while (tmpValidityNode != null)
      {
        if ((tmpValidityNode.validFrom <= cdrDate) &
            (tmpValidityNode.validTo > cdrDate))
        {
          custId = tmpValidityNode.custId;
          break;
        }

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }
    }

    // return the id
    return custId;
  }

 /**
  * Gets an internal custID for a given alias and date
  *
  * @param alias The alias for the customer account
  * @param cdrDate The date to recover the internal cust AuditSegID for
  * @return The internal customer AuditSegID
  */
  public String getSubscriptionId(String alias, long cdrDate)
  {
    ValidityNode tmpValidityNode;

    // See if we already have AuditSegID for this customer
    if (alias!= null && aliasCache.containsKey(alias))
    {
      // get the start of the search tree
      tmpValidityNode = aliasCache.get(alias);

      // Now that we have the Validity Map, get the entry
      while (tmpValidityNode != null)
      {
        if ((tmpValidityNode.validFrom <= cdrDate) &
            (tmpValidityNode.validTo > cdrDate))
        {
          return tmpValidityNode.subId;
        }

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }
    }

    // return the id
    return null;
  }
  
 /**
  * Gets the internal custIDs for a given alias (there can be more than one)
  *
  * @param alias The alias for the customer account
  * @return The internal customer AuditSegID
  */
  public ArrayList<Integer> getAllCustId(String alias)
  {
    Integer custId;
    ValidityNode tmpValidityNode;
    ArrayList<Integer> tmpResult = new ArrayList<>();

    // See if we already have AuditSegID for this customer
    if (aliasCache.containsKey(alias))
    {
      // get the start of the search tree
      tmpValidityNode = aliasCache.get(alias);

      // Now that we have the Validity Map, get the entry
      while (tmpValidityNode != null)
      {
        custId = tmpValidityNode.custId;
        tmpResult.add(custId);

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }
    }

    return tmpResult;
  }

 /**
  * Gets a the Subscription AuditSegID for a given alias and date
  *
  * @param alias The alias to get the subscription AuditSegID for
  * @param CDRDate The date to get the subscription AuditSegID for
  * @return The subscription AuditSegID for the alias and date
  */
  public String getSubId(String alias, long CDRDate)
  {
    String subId = null;
    ValidityNode tmpValidityNode;

    // See if we already have AuditSegID for this customer
    if (aliasCache.containsKey(alias))
    {
      // get the start of the search tree
      tmpValidityNode = aliasCache.get(alias);

      // Now that we have the Validity Map, get the entry
      while (tmpValidityNode != null)
      {
        if ((tmpValidityNode.validFrom <= CDRDate) &
            (tmpValidityNode.validTo > CDRDate))
        {
          subId = tmpValidityNode.subId;
          break;
        }

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }
    }

    return subId;
  }

 /**
  * Gets the audit segment for the CustId and the CDR date.
  *
  * @param custId The internal customer account AuditSegID to retrieve the audit seg for
  * @param CDRDate The date to retrieve for
  * @return The audit segment for the account and date
  */
  public AuditSegment getAuditSegment(Integer custId, long CDRDate)
  {
    CustInfo tmpCustInfo;
    AuditSegment tmpAuditSegment;

    // get the customer for the alias
    tmpCustInfo = custCache.get(custId);

    //get the correct audit segment
    tmpAuditSegment = tmpCustInfo.getBestAuditSegmentMatch(CDRDate);

    return tmpAuditSegment;
  }

 /**
  * Return the value of the balance group so that we are able to update
  * it during the logic processing. This is to make sure that we have all
  * the information necessary when we start the calculation.
  *
  * @param custID The internal customer AuditSegID to retrieve the balance group for
  * @return The balance group AuditSegID
  */
  public long getBalanceGroup(Integer custID)
  {
    CustInfo tmpCustInfo;

    // Get the product information
    tmpCustInfo = custCache.get(custID);

    return tmpCustInfo.balanceGroup;
  }

 /**
  * Return the value of the ExternalCustId associated with the Cust Id.
  *
  * @param custID The internal customer AuditSegID to get the ExternalCustId for
  * @return The ExternalCustId
  */
  public String getExtCustID(Integer custID)
  {
    CustInfo tmpCustInfo;

    // Get the product information
    tmpCustInfo = custCache.get(custID);

    return tmpCustInfo.ExternalCustId;
  }

 /**
  * Return the value of the ERA associated with an account.
  *
  * @param alias The alias to get the ERA for
  * @param eraKey The ERA key to get
  * @param cdrDate The date of the CDR
  * @return The ERA value
  * @throws ProcessingException
  */
  public String getERA(String alias, String eraKey, long cdrDate) throws ProcessingException
  {
    Integer custId = null;
    CustInfo tmpCustInfo;
    AuditSegment tmpAuditSegment;
    ValidityNode tmpValidityNode;

    // See if we already have AuditSegID for this customer
    if (aliasCache.containsKey(alias))
    {
      // get the start of the search tree
      tmpValidityNode = aliasCache.get(alias);

      // Now that we have the Validity Map, get the entry
      while (tmpValidityNode != null)
      {
        if ((tmpValidityNode.validFrom <= cdrDate) &
            (tmpValidityNode.validTo > cdrDate))
        {
          custId = tmpValidityNode.custId;
          break;
        }

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }

      if (custId == null)
      {
        // Otherwise write an error and ignore it
        String Message = "Alias <" + alias + "> not found for time <" + cdrDate + ">. Lookup failed.";
        throw new ProcessingException(Message);
      }
      else
      {
        // get the customer for the alias
        tmpCustInfo = custCache.get(custId);

        //get the correct audit segment
        tmpAuditSegment = tmpCustInfo.getBestAuditSegmentMatch(cdrDate);

        if (tmpAuditSegment != null)
        {
          FWlog.debug("Using audit segment <" + tmpAuditSegment.getAuditSegmentID() + "> for alias <" + alias + "> at time <" + cdrDate + ">");

          return tmpAuditSegment.getERA(eraKey);
        }
      }
    }

    return null;
  }

 /**
  * Return the value of the ERA associated with an account.
  *
  * @param tmpAuditSegment The audit segment to get the ERA from
  * @param eraKey The key of the ERA to get
  * @return The value of the ERA
  */
  public String getERA(AuditSegment tmpAuditSegment, String eraKey)
  {
    return tmpAuditSegment.getERA(eraKey);
  }

  // -----------------------------------------------------------------------------
  // -------------------- Start of data loading functions ------------------------
  // -----------------------------------------------------------------------------

 /**
  * loadCache is called automatically on startup of the
  * cache factory, as a result of implementing the CacheLoader
  * interface. This should be used to load any data that needs loading, and
  * to set up variables.
  *
  * @param ResourceName The name of the resource to load for
  * @param CacheName The name of the cache to load for
  * @throws InitializationException
  */
  @Override
  public void loadCache(String ResourceName, String CacheName)
                 throws InitializationException
  {
    String tmpFrequency;

    // Do the parent processing first
    super.loadCache(ResourceName, CacheName);


    // load the update frequency
    tmpFrequency = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                     CacheName,
                                                     SERVICE_UPDATE_FREQUENCY,
                                                     "None");
    if (!tmpFrequency.equalsIgnoreCase("None"))
    {
      // process it
      processControlEvent(SERVICE_UPDATE_FREQUENCY,true,tmpFrequency);
    }
  }

 /**
  * load the data from a file
  *
  * @throws InitializationException
  */
  @Override
  public synchronized void loadDataFromFile() throws InitializationException
  {
    // Variable declarations
    int              auditSegsLoaded = 0;
    int              cpiLoaded = 0;
    int              aliasLoaded = 0;
    int              eraLoaded = 0;
    int              lineCounter = 0;
    BufferedReader   inFile;
    String           tmpFileRecord;
    String[]         recordFields;
    SimpleDateFormat sdfInput = new SimpleDateFormat (internalDateFormat);
    long             audSegValidFrom;
    int              balGroup;
    Integer          custId = 0;
    String           tmpAudSegDate;
    String           tmpExtCustID;
    String           tmpCustID;
    String           tmpAuditSegID;

    String           tmpStrToDate;
    String           tmpStrFromDate;
    String           tmpProdName;
    String           tmpService;
    String           tmpSubID;
    String           tmpAlias;
    long             tmpToDate = CommonConfig.HIGH_DATE;
    long             tmpFromDate = CommonConfig.LOW_DATE;

    String           tmpCustToDate;
    String           tmpCustFromDate;
    long             custToDate = CommonConfig.HIGH_DATE;
    long             custFromDate = CommonConfig.LOW_DATE;
    long             AuditSegID;

    // Log that we are starting the loading
    FWlog.info("Starting Customer Cache Loading from File");

    // Try to open the file
    try
    {
      inFile = new BufferedReader(new FileReader(CacheDataFile));
    }
    catch (FileNotFoundException exFileNotFound)
    {
      FWlog.error(
            "Application is not able to read file : <" +
            cacheDataSourceName + ">");
      throw new InitializationException("Application is not able to read file: <" +
                                        CacheDataFile + ">",
                                        exFileNotFound);
    }

    // File open, now get the stuff
    try
    {
      while (inFile.ready())
      {
        tmpFileRecord = inFile.readLine();
        lineCounter++;

        if ((tmpFileRecord.startsWith("#")) |
            tmpFileRecord.trim().equals(""))
        {
          // Comment line, ignore
        }
        else
        {
            recordFields = tmpFileRecord.split(";");

            // Work on the different types of records in the file
            if ( recordFields[0].equals("01") )
            {
              // Customer data - prepare the fields
              tmpAuditSegID    = recordFields[0];
              tmpCustID        = recordFields[1];
              tmpExtCustID     = recordFields[2];
              tmpAudSegDate    = recordFields[3];
              tmpCustFromDate  = recordFields[4];
              tmpCustToDate    = recordFields[5];
              custId           = Integer.parseInt(tmpCustID);
              balGroup         = Integer.parseInt(tmpCustID);
              audSegValidFrom  = Long.parseLong(tmpAudSegDate);
              AuditSegID       = Long.parseLong(tmpAuditSegID);

              try
              {
                audSegValidFrom = sdfInput.parse(tmpAudSegDate).getTime()/1000;
                custFromDate = sdfInput.parse(tmpCustFromDate).getTime()/1000;
                custToDate = sdfInput.parse(tmpCustToDate).getTime()/1000;
              }
              catch (ParseException ex)
              {
                FWlog.error("Date formats for record <" + tmpFileRecord + "> on line <" + lineCounter + "> are not correct. Data discarded." );
              }

              addAuditSegment(AuditSegID,custId,tmpExtCustID,balGroup,audSegValidFrom,custFromDate,custToDate);
              auditSegsLoaded++;

              // Update status for long operations
              if ( (auditSegsLoaded % loadingLogNotificationStep) == 0)
              {
                FWlog.info("Customer Map Data Loaded " + auditSegsLoaded + " Customer Records");
              }
            }

            if (recordFields[0].equals("02"))
            {
              // Customer product - prepare the fields
              tmpAuditSegID  = recordFields[0];
              tmpProdName    = recordFields[1];
              tmpSubID       = recordFields[2];
              tmpService     = recordFields[3];
              tmpStrFromDate = recordFields[4];
              tmpStrToDate   = recordFields[5];
              AuditSegID     = Long.parseLong(tmpAuditSegID);

              try
              {
                tmpFromDate = sdfInput.parse(tmpStrFromDate).getTime()/1000;
                tmpToDate = sdfInput.parse(tmpStrToDate).getTime()/1000;
              }
              catch (ParseException ex)
              {
                FWlog.error("Date formats for record <" + tmpFileRecord + "> are not correct. Data discarded." );
              }

              addAuditedCPI(0, AuditSegID,tmpProdName,tmpSubID,tmpService,tmpFromDate,tmpToDate);
              cpiLoaded++;

              // Update status for long operations
              if ( (cpiLoaded % loadingLogNotificationStep) == 0)
              {
                FWlog.info("Customer Map Data Loaded " + cpiLoaded + " Product Records");
              }
            }

            if ( recordFields[0].equals("05") )
            {
              tmpAuditSegID  = recordFields[1];
              tmpAlias       = recordFields[2];
              tmpCustID      = recordFields[3];
              tmpSubID       = recordFields[4];
              tmpStrFromDate = recordFields[5];
              tmpStrToDate   = recordFields[6];
              AuditSegID     = Long.parseLong(tmpAuditSegID);

              try
              {
                tmpFromDate = sdfInput.parse(tmpStrFromDate).getTime()/1000;
                tmpToDate = sdfInput.parse(tmpStrToDate).getTime()/1000;
              }
              catch (ParseException ex)
              {
                FWlog.error("Date formats for record <" + tmpFileRecord + "> are not correct. Data discarded." );
              }

              // AuditSegID of 0 indicates that we do not allow update
              addAlias(AuditSegID,tmpAlias,custId,tmpSubID,tmpFromDate,tmpToDate);
              aliasLoaded++;
            }

            if ( recordFields[0].equals("06") )
            {
              tmpAuditSegID  = recordFields[1];
              tmpCustID      = recordFields[2];
              tmpSubID       = recordFields[3];
              tmpStrFromDate = recordFields[4];
              tmpStrToDate   = recordFields[5];
              AuditSegID     = Long.parseLong(tmpAuditSegID);

              addAuditedERA(AuditSegID,recordFields[2],recordFields[3]);
              eraLoaded++;
            }
            // Other types of record
            // ...
        }
      }
    }
    catch (IOException ex)
    {
      FWlog.fatal(
            "Error reading input file <" + CacheDataFile +
            "> in record <" + lineCounter + ">. IO Error.");
    }
    catch (ArrayIndexOutOfBoundsException ex)
    {
      FWlog.fatal(
            "Error reading input file <" + CacheDataFile +
            "> in record <" + lineCounter + ">. Malformed Record.");
    }
    finally
    {
      try
      {
        inFile.close();
      }
      catch (IOException ex)
      {
        FWlog.error("Error closing input file <" + CacheDataFile +
                  ">", ex);
      }
    }

    // set the last update time
    lastUpdate = System.currentTimeMillis();

    // finished
    FWlog.info(
          "Customer Cache Data Loading completed. " + lineCounter +
          " configuration lines loaded from <" + CacheDataFile +
          ">");
    FWlog.info("Alias Loaded:                  " + aliasLoaded);
    FWlog.info("CustomerAudit Segments Loaded: " + auditSegsLoaded);
    FWlog.info("Products Loaded:               " + cpiLoaded);
    FWlog.info("ERAs Loaded:                   " + eraLoaded);
  }

 /**
  * loadCache is called automatically on startup of the
  * cache factory, as a result of implementing the CacheLoader
  * interface.
  *
  * loadCache is also called when the cache is reloaded.
  *
  * @throws InitializationException
  */
  @Override
  public synchronized void loadDataFromDB() throws InitializationException
  {
    // Variable declarations
    int            aliasLoaded = 0;
    int            auditSegsLoaded = 0;
    int            cpiLoaded = 0;
    int            eraLoaded = 0;

    String         tmpAlias;
    String         tmpCustId;
    String         tmpSubId;
    String         tmpExtCustID;
    String         tmpProdName;

    Integer        custId;
    long           balGroup;
    long           auditSegID;
    long           aliasID;
    long           audSegValidFrom;
    long           fromDate;
    long           toDate;
    long           prodID;

    long           custToDate;
    long           custFromDate;
    long           modT;
    String         tmpService;
    String         tmpERAKey;
    String         tmpERAValue;

    // Find the location of the  zone configuration file
    FWlog.info("Starting Customer Cache Loading from DB");

    // Try - finally wrapper
    try
    {
      // Try to open the DS
      JDBCcon = DBUtil.getConnection(cacheDataSourceName);

      // Now prepare the statements
      prepareStatements();

      // set the where parameter to allow incremental loading
      try
      {
        stmtAliasSelectQuery.clearParameters();
        stmtAliasSelectQuery.setLong(1, lastAliasModT);
      }
      catch (SQLException ex)
      {
        String Message = "Error setting incremental ID for retieving customer " +
                        "alias data. SQL Error <" + ex.getMessage() + ">";
        throw new InitializationException(Message, ex);
      }

      // Execute the query
      try
      {
        crs = stmtAliasSelectQuery.executeQuery();
      }
      catch (SQLException ex)
      {
        String Message = "Error executing SQL for retieving customer alias data." +
                        " SQL Error <" + ex.getMessage() + ">";
        throw new InitializationException(Message, ex);
      }

      try
      {
        // loop through the results for the customer alias cache
        crs.next();
        if (crs.getRow() == 0)
        {
          if (lastUpdate == 0)
          {
            // warn that the initial load did not return any results
            FWlog.warning("No results found for customer alias data");
          }
        }
        else
        {
          do
          {
            aliasID         = crs.getLong(1);
            tmpAlias        = crs.getString(2);
            tmpCustId       = crs.getString(3);
            tmpSubId        = crs.getString(4);
            custFromDate    = crs.getLong(5);
            custToDate      = crs.getLong(6);
            modT            = crs.getLong(7);
            custId          = Integer.parseInt(tmpCustId);

            addAlias(aliasID,tmpAlias,custId,tmpSubId,custFromDate,custToDate);
            aliasLoaded++;

            // update the internal counter
            if (modT > lastAliasModT)
            {
              lastAliasModT = modT;
            }

            if ((aliasLoaded % loadingLogNotificationStep) == 0)
            {
              String Message = "Customer Cache Alias Loading: <" + aliasLoaded +
                    "> configuration lines loaded for <" + getSymbolicName() + "> from <" +
                    cacheDataSourceName + ">";
              getFWLog().info(Message);
            }
        } while (crs.next());
        }
      }
      catch (SQLException ex)
      {
        String Message = "Error retreiving alias data. SQL Error <" +
                        ex.getMessage() + ">";
        throw new InitializationException(Message, ex);
      }

      FWlog.info("Alias Loading completed. " + aliasLoaded +
        " configuration lines loaded from <" + cacheDataSourceName + ">");

      // set the where parameter to allow incremental loading
      try
      {
        stmtAuditSelectQuery.clearParameters();
        stmtAuditSelectQuery.setLong(1, lastAccountVerModT);
      }
      catch (SQLException ex)
      {
        String Message = "Error setting incremental ID for retieving customer " +
                        "audit data. SQL Error <" + ex.getMessage() + ">";
        throw new InitializationException(Message, ex);
      }

      // Execute the query
      try
      {
        ars = stmtAuditSelectQuery.executeQuery();
      }
      catch (SQLException ex)
      {
        String Message = "Error executing SQL for retieving customer audit data. " +
                        "SQL Error <" + ex.getMessage() + ">";
        throw new InitializationException(Message, ex);
      }

      // loop through the results for the audit segment cache
      try
      {
        // loop through the results for the audit segment cache
        ars.next();
        if (ars.getRow() == 0)
        {
          if (lastUpdate == 0)
          {
            // warn that the initial load did not return any results
            FWlog.warning("No results found for customer audit segment data");
          }
        }
        else
        {
          do
          {
            auditSegID      = ars.getLong(1);
            tmpCustId       = ars.getString(2);
            balGroup        = Long.parseLong(tmpCustId);
            tmpExtCustID    = ars.getString(3);
            audSegValidFrom = ars.getLong(4);
            custFromDate    = ars.getLong(5);
            custToDate      = ars.getLong(6);
            modT            = ars.getLong(7);
            custId          = Integer.parseInt(tmpCustId);

            addAuditSegment(auditSegID,custId,tmpExtCustID,balGroup,audSegValidFrom,custFromDate,custToDate);
            auditSegsLoaded++;

            // update the internal counter
            if (modT > lastAccountVerModT)
            {
              lastAccountVerModT = modT;
            }

            if ((auditSegsLoaded % loadingLogNotificationStep) == 0)
            {
              String Message = "Customer Cache Audit Segment Loading: <" + auditSegsLoaded +
                    "> configuration lines loaded for <" + getSymbolicName() + "> from <" +
                    cacheDataSourceName + ">";
              getFWLog().info(Message);
            }
          } while (ars.next());
        }
      }
      catch (SQLException ex)
      {
        String Message = "Error retreiving audit data. SQL Error <" + ex.getMessage() + ">";
        throw new InitializationException(Message, ex);
      }

      FWlog.info("Audit segment Loading completed. " + auditSegsLoaded +
        " configuration lines loaded from <" + cacheDataSourceName + ">");

      // set the where parameter to allow incremental loading
      try
      {
        stmtProductSelectQuery.clearParameters();
        stmtProductSelectQuery.setLong(1, lastProductModT);
      }
      catch (SQLException ex)
      {
        String Message = "Error setting incremental ID for retieving product data. " +
                        "SQL Error <" + ex.getMessage() + ">";
        throw new InitializationException(Message, ex);
      }

      // Execute the query
      try
      {
        prs = stmtProductSelectQuery.executeQuery();
      }
      catch (SQLException ex)
      {
        String Message = "Error executing SQL for retieving customer product data. " +
                        "SQL Error <" + ex.getMessage() + ">";
        throw new InitializationException(Message, ex);
      }

      // loop through the results for the product cache
      try
      {
        // loop through the results for the product cache
        prs.next();
        if (prs.getRow() == 0)
        {
          if (lastUpdate == 0)
          {
            // warn that the initial load did not return any results
            FWlog.warning("No results found for customer product data");
          }
        }
        else
        {
          do
          {
            auditSegID     = prs.getLong(1);
            prodID         = prs.getLong(2);
            tmpProdName    = prs.getString(3);
            tmpSubId       = prs.getString(4);
            tmpService     = prs.getString(5);
            fromDate       = prs.getLong(6);
            toDate         = prs.getLong(7);
            modT           = prs.getLong(8);

            addAuditedCPI(auditSegID,prodID,tmpProdName,tmpSubId,tmpService,fromDate,toDate);
            cpiLoaded++;

            // update the internal counter
            if (modT > lastProductModT)
            {
              lastProductModT = modT;
            }

            if ((cpiLoaded % loadingLogNotificationStep) == 0)
            {
              String Message = "Customer Cache Product Loading: <" + cpiLoaded +
                    "> configuration lines loaded for <" + getSymbolicName() + "> from <" +
                    cacheDataSourceName + ">";
              getFWLog().info(Message);
            }
          } while (prs.next());
        }
      }
      catch (SQLException ex)
      {
        String Message = "Error retreiving product data. SQL Error <" + ex.getMessage() + ">";
        throw new InitializationException(Message, ex);
      }

      // set the where parameter to allow incremental loading
      try
      {
        stmtERASelectQuery.clearParameters();
        stmtERASelectQuery.setLong(1, lastERAModT);
      }
      catch (SQLException ex)
      {
        String Message = "Error setting incremental ID for retieving ERA data. " +
                        "SQL Error <" + ex.getMessage() + ">";
        throw new InitializationException(Message, ex);
      }

      // Execute the query
      try
      {
        ers = stmtERASelectQuery.executeQuery();
      }
      catch (SQLException ex)
      {
        String Message = "Error executing SQL for retieving customer ERA data. " +
                        "SQL Error <" + ex.getMessage() + ">";
        throw new InitializationException(Message, ex);
      }

      // loop through the results for the era cache
      try
      {
        // loop through the results for the era cache
        ers.next();
        if (ers.getRow() == 0)
        {
          if (lastUpdate == 0)
          {
            // warn that the initial load did not return any results
            FWlog.warning("No results found for customer ERA data");
          }
        }
        else
        {
          do
          {
            auditSegID     = ers.getLong(1);
            tmpERAKey      = ers.getString(2);
            tmpERAValue    = ers.getString(3);
            modT           = ers.getLong(4);

            addAuditedERA(auditSegID,tmpERAKey,tmpERAValue);
            eraLoaded++;

            // update the internal counter
            if (modT > lastERAModT)
            {
              lastERAModT = modT;
            }
          } while (ers.next());
        }
      }
      catch (SQLException ex)
      {
        String Message = "Error retreiving ERA data. SQL Error <" + ex.getMessage() + ">";
        throw new InitializationException(Message, ex);
      }
    }
    finally
    {
      // We pass through this block in any case - whether an Exception was
      // thrown or not, to perform clean up
      
      // Close down result sets
      DBUtil.close(crs);
      DBUtil.close(ars);
      DBUtil.close(prs);
      DBUtil.close(ers);

      // close down statements
      closeStatements();

      // Close the DB connection
      DBUtil.close(JDBCcon);
    }

    // set the last update time
    lastUpdate = System.currentTimeMillis();

    // finished
    FWlog.info(
          "Customer Cache Data Loading completed from <" + cacheDataSourceName + ">");
    FWlog.info("Alias Loaded:                  " + aliasLoaded);
    FWlog.info("CustomerAudit Segments Loaded: " + auditSegsLoaded);
    FWlog.info("Products Loaded:               " + cpiLoaded);
    FWlog.info("ERAs Loaded:                   " + eraLoaded);
  }

 /**
  * Load the data from the defined Data Source Method
  *
  * @throws InitializationException
  */
  @Override
  public void loadDataFromMethod() throws InitializationException
  {
    throw new InitializationException("Not implemented yet");
  }

 /**
  * Reset the cache
  */
  @Override
  public void clearCacheObjects()
  {
    // clear the cache
    aliasCache.clear();
    custCache.clear();
    auditSegmentCache.clear();

    // reset the incremental counters
    lastAccountVerModT = 0;
    lastAliasModT = 0;
    lastERAModT = 0;
    lastProductModT = 0;
  }

 /**
  * This function sees if it is yet time to perform an update from the
  * customer database, and if so, performs the update
  */
  public void checkUpdate()
  {
    if (System.currentTimeMillis() > (lastUpdate + updateFrequency * 1000))
    {
      // perform the reload
      if (CacheDataSourceType.equalsIgnoreCase("DB"))
      {
        try
        {
          loadDataFromDB();
        }
        catch (InitializationException ex)
        {
          getFWLog().error("Error performing incremental update: " + ex.getMessage());
        }
      }
      else
      {
        getFWLog().error("Cannot perform incremental update from file source");
      }
    }
  }

 /**
  * Write the internal memory of the customer to a file that can be used
  * to debug the processing.
  *
  * @param alias The alias to dump for or "All" for all information
  * @return The name of the dump file that was written
  */
  public String DumpCacheData(String alias)
  {
    ArrayList<Integer> custIds;
    BufferedWriter     dumpWriter;
    FileWriter         fwriter = null;
    File               file;
    String             filename;
    Iterator<String>   aliasIter;
    Iterator<Integer>  IDIter;
    Integer            custId;
    String             aliasId;
    String             dumpString;

    // Create the file name and open the file
    filename = "CustomerInfo_" + alias + ".custinfo";
    file = new File(filename);

    try
    {
      if (file.createNewFile() == false)
      {
        getFWLog().error("Error creating file <" + filename + ">");
      }

      fwriter = new FileWriter(file);
    }
    catch (IOException ex)
    {
      getFWLog().error("Error opening dump file <" + filename + ">. Message <" + ex.getMessage() +">");
    }

    dumpWriter = new BufferedWriter(fwriter);

    // do the dumping
    if (alias.equalsIgnoreCase("all"))
    {
      // dump all the alias data
      dumpString = "==== Alias Information ====";
      try
      {
        dumpWriter.write(dumpString);
        dumpWriter.newLine();
      }
      catch (IOException ioe)
      {
        getFWLog().error("Error writing dump file", ioe);
      }

      aliasIter = aliasCache.keySet().iterator();
      while (aliasIter.hasNext())
      {
        aliasId = aliasIter.next();
        writeAliasInfo(aliasId,dumpWriter);
      }

      // Dump the customer data
      dumpString = "==== Customer Information ====";
      try
      {
        dumpWriter.newLine();
        dumpWriter.write(dumpString);
        dumpWriter.newLine();
      }
      catch (IOException ioe)
      {
        getFWLog().error("Error writing dump file", ioe);
      }
      IDIter = custCache.keySet().iterator();

      while (IDIter.hasNext())
      {
        custId = IDIter.next();
        writeCustInfo(custId,dumpWriter);
      }
    }
    else
    {
      // dump the alias data
      dumpString = "==== Alias Information ====";
      try
      {
        dumpWriter.write(dumpString);
        dumpWriter.newLine();
      }
      catch (IOException ioe)
      {
        getFWLog().error("Error writing dump file", ioe);
      }

      writeAliasInfo(alias,dumpWriter);

      // Dump the customer data
      dumpString = "==== Customer Information ====";
      try
      {
        dumpWriter.newLine();
        dumpWriter.write(dumpString);
        dumpWriter.newLine();
      }
      catch (IOException ioe)
      {
        getFWLog().error("Error writing dump file", ioe);
      }
      
      // Get a list of all the Cust IDs that have used the alias
      custIds = getAllCustId(alias);
      IDIter = custIds.iterator();

      while (IDIter.hasNext())
      {
        custId = IDIter.next();
        writeCustInfo(custId,dumpWriter);
      }
    }

    // close files
    try
    {
      if (dumpWriter != null)
      {
        dumpWriter.close();
      }
    }
    catch (IOException ioe)
    {
      getFWLog().error("Error closing dump file", ioe);
    }

    return filename;
  }

 /**
  * Write the information for one customer, including all subordinate infomation
  * (Audit Segments, Products and ERAs)
  *
  * @param custId The CustId to write
  * @param dumpWriter The dump writer to write to
  */
  private void writeCustInfo(Integer custId, BufferedWriter dumpWriter)
  {
    String       DumpString;
    CustInfo     tmpCustInfo;
    AuditSegment tmpAuditSegment;
    ProductList  tmpProductList;
    int          i;
    int          j;
    CustProductInfo tmpCPI;
    Iterator<String>     ERAIter;
    String       ERAKey;

    String       FromDate;
    String       ToDate;
    String       EffDate;

    try
    {
      DumpString = "Customer ID " + custId;
      dumpWriter.write(DumpString);
      dumpWriter.newLine();

      tmpCustInfo = custCache.get(custId);

      FromDate = fieldInterpreter.formatLongDate(tmpCustInfo.custValidFrom);
      ToDate = fieldInterpreter.formatLongDate(tmpCustInfo.custValidTo);

      DumpString = "  Valid from " + tmpCustInfo.custValidFrom + " (" + FromDate +
                   ") to " + tmpCustInfo.custValidTo + " (" + ToDate + ")";

      dumpWriter.write(DumpString);
      dumpWriter.newLine();

      DumpString = "  Balance group " + tmpCustInfo.balanceGroup;
      dumpWriter.write(DumpString);
      dumpWriter.newLine();

      DumpString = "  ExtCustID " + tmpCustInfo.ExternalCustId;
      dumpWriter.write(DumpString);
      dumpWriter.newLine();

      // Now write the audit segment stuff
      for (i = 0 ; i < tmpCustInfo.CustAudSegments.size() ; i++)
      {
        tmpAuditSegment = tmpCustInfo.CustAudSegments.get(i);

        // Output the segment information
        EffDate = fieldInterpreter.formatLongDate(tmpAuditSegment.getUTCSegmentValidFrom());
        FromDate = fieldInterpreter.formatLongDate(tmpAuditSegment.getUTCAccountValidFrom());
        ToDate = fieldInterpreter.formatLongDate(tmpAuditSegment.getUTCAccountValidTo());

        DumpString = "  Audit Segment ID " + tmpAuditSegment.getAuditSegmentID() +
                     " effective from " + tmpAuditSegment.getUTCSegmentValidFrom() + " (" +
                     EffDate + ") valid from " + tmpAuditSegment.getUTCAccountValidFrom() + " (" +
                     FromDate + ") to " + tmpAuditSegment.getUTCAccountValidFrom() + " (" + ToDate + ")";

        dumpWriter.write(DumpString);
        dumpWriter.newLine();

        // Output the products
        tmpProductList = tmpAuditSegment.getCustAudProds();
        for ( j=0 ; j < tmpProductList.getProductCount() ; j++)
        {
          tmpCPI = tmpProductList.getProduct(j);

          FromDate = fieldInterpreter.formatLongDate(tmpCPI.getUTCValidFrom());
          ToDate = fieldInterpreter.formatLongDate(tmpCPI.getUTCValidTo());
          DumpString = "    Product ID " + tmpCPI.getProductID() + " for service " +
                       tmpCPI.getService() + " subscription " + tmpCPI.getSubID() +
                       " valid from " + tmpCPI.getUTCValidFrom() + " (" + FromDate +
                       ") to " + tmpCPI.getUTCValidTo() + " (" + ToDate + ")";
          dumpWriter.write(DumpString);
          dumpWriter.newLine();
        }

        // Output the ERAs
        ERAIter = tmpAuditSegment.getERAs().keySet().iterator();
        while (ERAIter.hasNext())
        {
          ERAKey = ERAIter.next();
          DumpString = "    ERA " + ERAKey + " has value " + tmpAuditSegment.getERA(ERAKey);
          dumpWriter.write(DumpString);
          dumpWriter.newLine();
        }
      }
    }
    catch (IOException ex)
    {
      getFWLog().error("Error writing to dump file. Message <" + ex.getMessage() + ">");
    }
  }

 /**
  * Write the information for an alias, which may be a single custId, or multiple
  * in the case that the alias has been associated with various CustIds over
  * time
  *
  * @param CustId The alias to write
  * @param dumpWriter The dump writer to write to
  */
  private void writeAliasInfo(String aliasId, BufferedWriter dumpWriter)
  {
    String     DumpString;
    ValidityNode tmpValidityNode;

    try
    {
      // Write the header
      DumpString = "Alias ID " + aliasId;
      dumpWriter.write(DumpString);
      dumpWriter.newLine();

      // Write the information for the alias history
      tmpValidityNode = aliasCache.get(aliasId);
      while (tmpValidityNode != null)
      {
        String FromDate = fieldInterpreter.formatLongDate(tmpValidityNode.validFrom);
        String ToDate = fieldInterpreter.formatLongDate(tmpValidityNode.validTo);

        DumpString = "  Associated with Cust ID " + tmpValidityNode.custId +
                " from " + tmpValidityNode.validFrom + " (" + FromDate +
                ") to " + tmpValidityNode.validTo + " (" + ToDate +
                ") for Subscription " + tmpValidityNode.subId;
        dumpWriter.write(DumpString);
        dumpWriter.newLine();

        // Move down the map
        tmpValidityNode = tmpValidityNode.child;
      }
    }
    catch (IOException ex)
    {
      getFWLog().error("Error writing to dump file. Message <" + ex.getMessage() + ">");
    }
  }

  // -----------------------------------------------------------------------------
  // ---------------- Start of data base data layer functions --------------------
  // -----------------------------------------------------------------------------

 /**
  * get the select statement(s). Implemented as a separate function so that it can
  * be overwritten in implementation classes. By default the cache picks up the
  * statement with the name "SelectStatement".
  *
  * @param ResourceName The name of the resource to load for
  * @param CacheName The name of the cache to load for
  * @return True if the statements were found, otherwise false
  * @throws InitializationException
  */
  @Override
  protected boolean getDataStatements(String ResourceName, String CacheName)
    throws InitializationException
  {
    // Get the Select statement
    aliasSelectQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                     CacheName,
                                                                     "AliasSelectStatement",
                                                                     "None");
    
    if (aliasSelectQuery.equalsIgnoreCase("None"))
    {
      String Message = "<AliasSelectStatement> for <" + getSymbolicName() + "> missing.";
      throw new InitializationException(Message);      
    }

    auditSelectQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                        CacheName,
                                                                        "AuditSegmentSelectStatement",
                                                                        "None");

    if (auditSelectQuery.equalsIgnoreCase("None"))
    {
      String Message = "<AuditSegmentSelectStatement> for <" + getSymbolicName() + "> missing.";
      throw new InitializationException(Message);      
    }
    
    productSelectQuery = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                       CacheName,
                                                                       "ProductSelectStatement",
                                                                       "None");

    if (productSelectQuery.equalsIgnoreCase("None"))
    {
      String Message = "<ProductSelectStatement> for <" + getSymbolicName() + "> missing.";
      throw new InitializationException(Message);      
    }
    
    eraSelectQuery         = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(ResourceName,
                                                                       CacheName,
                                                                       "ERASelectStatement",
                                                                       "None");

    if (eraSelectQuery.equalsIgnoreCase("None"))
    {
      String Message = "<ERASelectStatement> for <" + getSymbolicName() + "> missing.";
      throw new InitializationException(Message);      
    }
    
    if (aliasSelectQuery.equals("None")  |
        auditSelectQuery.equals("None")  |
        productSelectQuery.equals("None")|
        eraSelectQuery.equals("None"))
    {
      return false;
    }
    else
    {
      return true;
    }
  }

 /**
  * PrepareStatements creates the statements from the SQL expressions
  * so that they can be run as needed.
  *
  * @throws InitializationException
  */
  @Override
  protected void prepareStatements() throws InitializationException
  {
    try
    {
      // prepare the SQL for the Alias Statement
      stmtAliasSelectQuery = JDBCcon.prepareStatement(aliasSelectQuery,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }
    catch(SQLException ex)
    {
      FWlog.error("Error preparing the statement " + aliasSelectQuery);
      FWlog.error("SQL Error:" + ex.getMessage());
      throw new InitializationException("Error preparing the statement <" +
        aliasSelectQuery + ">");
    }

    try
    {
      // prepare the SQL for the Audit Statement
      stmtAuditSelectQuery = JDBCcon.prepareStatement(auditSelectQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }
    catch(SQLException ex)
    {
      FWlog.error("Error preparing the statement " + auditSelectQuery);
      throw new InitializationException("Error preparing the statement <" +
        auditSelectQuery + ">");
    }

    try
    {
      // prepare the SQL for the Product Statement
      stmtProductSelectQuery = JDBCcon.prepareStatement(productSelectQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }
    catch(SQLException ex)
    {
      FWlog.error("Error preparing the statement " + productSelectQuery);
      throw new InitializationException("Error preparing the statement <" +
        productSelectQuery + ">");
    }

    try
    {
      // prepare the SQL for the ERA Statement
      stmtERASelectQuery = JDBCcon.prepareStatement(eraSelectQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }
    catch(SQLException ex)
    {
      FWlog.error("Error preparing the statement " + eraSelectQuery);
      throw new InitializationException("Error preparing the statement <" +
        eraSelectQuery + ">");
    }
  }
  
 /**
  * closeStatements closes down the the statements from the SQL expressions.
  *
  */
  protected void closeStatements()
  {
    // prepare the SQL for the Alias Statement
    DBUtil.close(stmtAliasSelectQuery);

      // prepare the SQL for the Audit Statement
    DBUtil.close(stmtAuditSelectQuery);

    // prepare the SQL for the Product Statement
    DBUtil.close(stmtProductSelectQuery);

    // prepare the SQL for the ERA Statement
    DBUtil.close(stmtERASelectQuery);
  }
  
  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------

 /**
  * registerClientManager registers the client module to the ClientManager class
  * which manages all the client modules available in this OpenRate Application.
  *
  * registerClientManager registers this class as a client of the ECI listener
  * and publishes the commands that the plug in understands. The listener is
  * responsible for delivering only these commands to the plug in.
  *
  */
  @Override
  public void registerClientManager() throws InitializationException
  {
    super.registerClientManager();

    //Register services for this Client
    ClientManager.registerClientService(getSymbolicName(), SERVICE_UPDATE_FREQUENCY, ClientManager.PARAM_DYNAMIC_SYNC);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_DUMP_INFO, ClientManager.PARAM_DYNAMIC);
  }

 /**
  * processControlEvent is the method that will be called when an event
  * is received for a module that has registered itself as a client of the
  * External Control Interface
  *
  * @param Command - command that is understand by the client module
  * @param Init - we are performing initial configuration if true
  * @param Parameter - parameter for the command
  * @return The result string of the operation
  */
  @Override
  public String processControlEvent(String Command, boolean Init,
                                    String Parameter)
  {
    int         ResultCode = -1;
    String      FileName;

    if (Command.equalsIgnoreCase(SERVICE_UPDATE_FREQUENCY))
    {
      if (Parameter.equalsIgnoreCase(""))
      {
        // return the current value
        return String.valueOf(updateFrequency);
      }
      else
      {
        // try to update
        try
        {
          updateFrequency = Integer.parseInt(Parameter);
        }
        catch (NumberFormatException nfe)
        {
          return "<" + Parameter + "> is not a valid integer value";
        }

        ResultCode = 0;
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_DUMP_INFO))
    {
      if (Parameter.equalsIgnoreCase("All"))
      {
        // dump all cache data
        FileName = DumpCacheData("All");
        return "Wrote file <" + FileName + ">";
      }
      else if (Parameter.length() > 0)
      {
        // dump only a specific alias
        FileName = DumpCacheData(Parameter);
        return "Wrote file <" + FileName + "> for alias <" + Parameter + ">";
      }
      else
      {
        // We did not get a parameter
        return "Operation <" + SERVICE_DUMP_INFO + "> requires a parameter 'All' or a specific Alias";
      }
    }

    if (ResultCode == 0)
    {
      getFWLog().debug(LogUtil.LogECICacheCommand(getSymbolicName(), Command, Parameter));

      return "OK";
    }
    else
    {
      return super.processControlEvent(Command, Init, Parameter);
    }
  }
}
