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

package OpenRate.record;

import OpenRate.lang.CustProductInfo;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * A Record corresponds to a unit of work that is being processed by the
 * pipeline. Records are created in the InputAdapter, pass through the Pipeline,
 * and written out in the OutputAdapter. Any stage of the pipeline my update
 * the record in any way, provided that later stages in the processing and the
 * output adapter know how to treat the record they receive.
 *
 * As an alternative, you may define a less flexible record format as you wish
 * and fill in the fields as required, but this costs performance.
 *
 * Generally, the record should know how to handle the following operations by
 * linking the appropriate method:
 *
 * mapOriginalData()   [mandatory]
 * -----------------
 * Transformation from a flat record as read by the input adapter to a formatted
 * record.
 *
 * unmapOriginalData() [mandatory if you wish to write output files]
 * -------------------
 * Transformation from a formatted record to a flat record ready for output.
 *
 * getDumpInfo()       [optional]
 * -------------
 * Preparation of the dump equivalent of the formatted record, ready for
 * dumping out to a dump file.
 *
 * In this simple example, we require only to read the "B-Number", and write the
 * "Destination" as a result of this. Because of the simplicity of the example
 * we do not perform a full mapping, we just handle the fields we want directly,
 * which is one of the advantages of the BBPA model (map as much as you want or
 * as little as you have to).
 *
 */
public abstract class RatingRecord extends AbstractRecord implements IRatingRecord
{
  /**
   * Serial UID for serialised object
   */
  private static final long serialVersionUID = 5417534942969198413L;

  /**
   * The split fields of the record
   */
  public String[] fields;

  /**
   * This holds the original data
   */
  public String OriginalData;

  /**
   * Worker variables to save references during processing.
   */
  public String Service = null;

  /**
   * The candidate rate plans that this customer has
   */
  public ArrayList<CustProductInfo> RatePlans = new ArrayList<>();

  // Rating information for performing internal calculations
  private final ArrayList<ChargePacket> ChargePackets = new ArrayList<>();

  // Rating information for updating the DB
  private final ArrayList<BalanceImpact> BalanceImpacts = new ArrayList<>();

  /**
   * RUM information - holds the list of RUMs and their values
   */
  public ArrayList<RUMInfo> RUMs = new ArrayList<>();

  /**
   * This is the counter index for monthly counters, usually filled with YYYYMM
   */
  public int CounterCycle;

  /**
   * This is the event start date
   */
  public Date EventStartDate;

  /**
   * This is the event end date
   */
  public Date EventEndDate;

  /**
   * This is the event date as a UTC date
   */
  public long UTCEventDate;

  /**
   * this controls the creation of the charge breakdown
   */
  public boolean CreateBreakdown = true;

 /**
  * Default Constructor for RateRecord, creating the empty record container
  */
  public RatingRecord()
  {
    super();
  }

 /**
  * Overloaded Constructor for RateRecord, creating the empty record container
  * and initialising the input data.
  * 
   * @param originalData
  */
  public RatingRecord(String originalData)
  {
    super();

    // Set the data
    this.setOriginalData(originalData);
  }

 /**
  * Get the charge packets that are available. This is implemented as
  * a function to allow the charge packet definition to be overwritten.
  *
  * @return The charge packets
  */
  public ArrayList<ChargePacket> getChargePackets()
  {
    return this.ChargePackets;
  }

 /**
  * Get the number of charge packets that are available. This is implemented as
  * a function to allow the charge packet definition to be overwritten.
  *
  * @return The number of charge packets
  */
  public int getChargePacketCount()
  {
    return this.ChargePackets.size();
  }

 /**
  * This returns the charge packet at the given index.
  *
  * @param Index The index to use for the retrieval
  * @return The requested charge packet
  */
  public ChargePacket getChargePacket(int Index)
  {
    return this.ChargePackets.get(Index);
  }

 /**
  * Adds a charge packet
  *
  * @param newCP The new Charge Packet to be added
  */
  public void addChargePacket(ChargePacket newCP)
  {
    this.ChargePackets.add(newCP);
  }

 /**
  * Create a charge packet
  *
  * @return the new charge packet
  */
  public ChargePacket newChargePacket()
  {
    return new ChargePacket();
  }

 /**
  * Replace a previous set of charge packets with a new one
  *
  * @param tmpCPList The new Charge Packets to use
  */
  public void replaceChargePackets(ArrayList<ChargePacket> tmpCPList)
  {
    this.ChargePackets.clear();
    this.ChargePackets.addAll(tmpCPList);
  }

 /**
  * Get the number of balance impacts that are available. This is implemented as
  * a function to allow the charge packet definition to be overwritten.
  *
  * @return The number of balance impacts
  */
  @Override
  public int getBalanceImpactCount()
  {
    return this.BalanceImpacts.size();
  }

 /**
  * This returns the balance impact at the given index.
  *
  * @param Index The index to use for the retreival
  * @return The requested balance impact
  */
  @Override
  public BalanceImpact getBalanceImpact(int Index)
  {
    return this.BalanceImpacts.get(Index);
  }

  /* This returns all balance impacts.
  *
  * @return The requested balance impacts
  */
  @Override
  public List<BalanceImpact> getBalanceImpacts() {
    return this.BalanceImpacts;
  }
  
 /**
  * Adds a balance impact
  *
  * @param newBI The new balance impact to add
  */
  @Override
  public void addBalanceImpact(BalanceImpact newBI)
  {
    this.BalanceImpacts.add(newBI);
  }

 /**
  * Create a charge packet
  *
  * @return the new charge packet
  */
  @Override
  public BalanceImpact newBalanceImpact()
  {
    return new BalanceImpact();
  }

 /**
  * Replace a previous set of charge packets with a new one
  *
  * @param tmpBIList The new Balance Impacts to use
  */
  public void replaceBalanceImpacts(ArrayList<BalanceImpact> tmpBIList)
  {
    this.BalanceImpacts.clear();
    this.BalanceImpacts.addAll(tmpBIList);
  }

 /**
  * Utility function to return the field at the index given.
  *
  * @param Index The index of the field to return
  * @return The returned value
  */
  public String getField(int Index)
  {

    return fields[Index];
  }

 /**
  * Utility function to set the field at the index given.
  *
  * @param Index The index of the field to set
  * @param NewValue The new value to set
  */
  public void setField(int Index, String NewValue)
  {
    fields[Index] = NewValue;
  }

 /**
  * Return the original input data
  *
  * @return The original data that was mapped
  */
  public final String getOriginalData()
  {
    return this.OriginalData;
  }

 /**
  * Utility function to return the field at the index given.
  *
  * @param NewData The data to set
  */
  public final void setOriginalData(String NewData)
  {
    this.OriginalData = NewData;
  }

 /**
  * Get the value of an existing RUM, or 0 if not found
  *
  * @param RUM The RUM value to get
  * @return The current value of the RUM
  */
  @Override
  public double getRUMValue(String RUM)
  {
    RUMInfo tmpRUM;
    int Index;

    for (Index = 0 ; Index < RUMs.size() ; Index++)
    {
      tmpRUM = RUMs.get(Index);

      if (tmpRUM.RUMName.equals(RUM))
      {
        return tmpRUM.RUMQuantity;
      }
    }

    return 0;
  }

 /**
  * Get the existing RUM values
  *
  * @return The current RUM list
  */
  @Override
  public List<RUMInfo> getRUMs() {
    return RUMs;
  }

 /**
  * Get the original value of an existing RUM, or 0 if not found
  *
  * @param RUM The RUM value to get
  * @return The current value of the RUM
  */
  public double getOriginalRUMValue(String RUM)
  {
    RUMInfo tmpRUM;
    int Index;

    for (Index = 0 ; Index < RUMs.size() ; Index++)
    {
      tmpRUM = RUMs.get(Index);

      if (tmpRUM.RUMName.equals(RUM))
      {
        return tmpRUM.OrigQuantity;
      }
    }

    return 0;
  }

 /**
  * Set the value of a RUM, return true if OK, false if not OK (e.g. overwrite
  * existing value)
  *
  * @param RUM The RUM value to set
  * @param newValue The new value to set
  */
  @Override
  public void setRUMValue(String RUM, double newValue)
  {
    RUMInfo tmpRUM;
    int Index;

    for (Index = 0 ; Index < RUMs.size() ; Index++)
    {
      tmpRUM = RUMs.get(Index);

      if (tmpRUM.RUMName.equals(RUM))
      {
        tmpRUM.RUMQuantity = newValue;
        return;
      }
    }

    tmpRUM = new RUMInfo(RUM,newValue);

    RUMs.add(tmpRUM);
  }

 /**
  * Set the value of a RUM, return true if OK, false if not OK (e.g. overwrite
  * existing value)
  *
  * @param RUM The RUM value to set
  * @param ValueDelta The delta to apply to the RUM value
  * @return true if the delta was applied, otherwise false
  */
  @Override
  public boolean updateRUMValue(String RUM, double ValueDelta)
  {
    RUMInfo tmpRUM;
    int Index;

    for (Index = 0 ; Index < RUMs.size() ; Index++)
    {
      tmpRUM = RUMs.get(Index);

      if (tmpRUM.RUMName.equals(RUM))
      {
        tmpRUM.RUMQuantity += ValueDelta;
        return true;
      }
    }

    return false;
  }

 /**
  * Utility function to set the counter cycle
  *
  * @param NewValue The new value to set
  */
  public void setCounterCycle(int NewValue)
  {
    CounterCycle = NewValue;
  }

 /**
  * Utility function to set the counter cycle
  *
  * @return The counter cycle value
  */
  public int getCounterCycle()
  {
    return CounterCycle;
  }

 /**
  * Get the total impacts for a given resource. Excludes invalid charge
  * packets
  *
  * @param resourceToGet The name of the resource to recover the total for
  * @return The total value of the all impacts for the defined resource
  */
  public double getTotalImpact(String resourceToGet)
  {
    int Index;
    ChargePacket tmpCP;
    double Total = 0;

    for (Index = 0 ; Index < ChargePackets.size() ; Index++)
    {
      tmpCP = ChargePackets.get(Index);

      if (tmpCP.Valid)
      {
        if (tmpCP.resource.equals(resourceToGet))
        {
          Total += tmpCP.chargedValue;
        }
      }
    }

    return Total;
  }

 /**
  * Get the total impacts for a given resource and Packet Type. Excludes invalid
  * charge packets
  *
  * @param resourceToGet The name of the resource to recover the total for
  * @param packetType The type of packet to recover the total for
  * @return The total value of the all impacts for the defined resource
  */
  public double getTotalImpact(String resourceToGet, String packetType)
  {
    int Index;
    ChargePacket tmpCP;
    double Total = 0;

    for (Index = 0 ; Index < ChargePackets.size() ; Index++)
    {
      tmpCP = ChargePackets.get(Index);

      if (tmpCP.Valid)
      {
        if (tmpCP.resource.equals(resourceToGet) && tmpCP.packetType.equals(packetType))
        {
          Total += tmpCP.chargedValue;
        }
      }
    }

    return Total;
  }

 /**
  * Get the resources that have been impacted in this record
  *
  * @return The list of resources that have been impacted
  */
  public ArrayList<String> getImpactResources()
  {
    int            Index;
    ChargePacket   tmpCP;
    ArrayList<String> ResultList = new ArrayList<>();
    String         Resource;
    Iterator<String>       ResourceIterator;
    boolean        Found = false;

    for (Index = 0 ; Index < ChargePackets.size() ; Index++)
    {
      tmpCP = ChargePackets.get(Index);

      Resource = tmpCP.resource;

      ResourceIterator = ResultList.iterator();

      while (ResourceIterator.hasNext())
      {
        if (Resource.equals(ResourceIterator.next()))
        {
          // already have this resource in the list
          Found = true;
          break;
        }
      }

      if (!Found)
      {
        ResultList.add(Resource);
      }
    }

    return ResultList;
  }

 /**
  * Get the UTC event date of the record
  *
  * @return The long UTC event date of the record
  */
  @Override
  public long getUTCEventDate()
  {
    return UTCEventDate;
  }

 /**
  * Get the formatted dump of the charge packets associated with this record.
  * Defaults the padding.
  *
  * @return Charge Packets Dump Information
  */
  public ArrayList<String> getChargePacketsDump()
  {
    return getChargePacketsDump(24);
  }

 /**
  * Get the formatted dump of the charge packets associated with this record.
  *
  * @param padding the number of characters to pad to
  * @return Charge Packets Dump Information
  */
  public ArrayList<String> getChargePacketsDump(int padding)
  {
    ChargePacket tmpCP;
    ArrayList<String> tmpDumpList = new ArrayList<>();

    // set up the padding
    String pad;
    if (padding < 19) {
      pad = "                                                  ".substring(1, 1);
    } else {
      pad = "                                                  ".substring(1, padding - 18);
    }

    int tmpChargePacketCount = this.getChargePacketCount();
    tmpDumpList.add("  Charge Packets   " + pad + "= <" + tmpChargePacketCount + ">");
    if (tmpChargePacketCount>0)
    {
      tmpDumpList.add("------------ Charge Packets ---------------");
      for (int i = 0 ; i < tmpChargePacketCount ; i++)
      {
        tmpCP = this.getChargePacket(i);
        
        // Add the breakdowns
        if (tmpCP != null)
        {
          tmpDumpList.add("    Rate Plan Name " + pad + "= <" + tmpCP.ratePlanName + ">");
          tmpDumpList.add("    Packet Type    " + pad + "= <" + tmpCP.packetType + ">");
          tmpDumpList.add("    Service        " + pad + "= <" + tmpCP.service + ">");
          tmpDumpList.add("    SubscriptionID " + pad + "= <" + tmpCP.subscriptionID + ">");
          tmpDumpList.add("    Priority       " + pad + "= <" + tmpCP.priority + ">");
          tmpDumpList.add("    RUM Name       " + pad + "= <" + tmpCP.rumName + ">");
          tmpDumpList.add("    RUM Value      " + pad + "= <" + tmpCP.rumQuantity + ">");
          tmpDumpList.add("    Resource       " + pad + "= <" + tmpCP.resource + ">");
          tmpDumpList.add("    Resource ID    " + pad + "= <" + tmpCP.resCounter + ">");
          tmpDumpList.add("    Time Model     " + pad + "= <" + tmpCP.timeModel + ">");
          tmpDumpList.add("    Zone Model     " + pad + "= <" + tmpCP.zoneModel + ">");
          tmpDumpList.add("    Zone Result    " + pad + "= <" + tmpCP.zoneResult + ">");
          tmpDumpList.add("    Rating Type    " + pad + "= <" + tmpCP.ratingType + ">");
          tmpDumpList.add("    Rating Desc    " + pad + "= <" + tmpCP.ratingTypeDesc + ">");
          tmpDumpList.add("    Rated Value    " + pad + "= <" + tmpCP.chargedValue + ">");
          tmpDumpList.add("    Splitting      " + pad + "= <" + tmpCP.timeSplitting + ">");
          tmpDumpList.add("    ----------------");
            
          if (tmpCP.getTimeZones() != null)
          {
            for (TimePacket tmpTz : tmpCP.getTimeZones()) {
              tmpDumpList.add("      Time Result  " + pad + "= <" + tmpTz.timeResult + ">");              
              tmpDumpList.add("      Duration     " + pad + "= <" + tmpTz.duration + ">");
              tmpDumpList.add("      Total Dur    " + pad + "= <" + tmpTz.totalDuration + ">");
              tmpDumpList.add("      Price Group  " + pad + "= <" + tmpTz.priceGroup + ">");
              tmpDumpList.add("      Price Model  " + pad + "= <" + tmpTz.priceModel + ">");
              tmpDumpList.add("      ----------------");
            }
          } else {
            tmpDumpList.add("	----- NO TZ -----");
          }

              
          if (tmpCP.breakDown != null)
          {
            for (RatingBreakdown tmpRB : tmpCP.breakDown) {
              tmpDumpList.add("      Step number  " + pad + "= <" + tmpRB.stepUsed + ">");
              tmpDumpList.add("      Tier from    " + pad + "= <" + tmpRB.tierFrom + ">");
              tmpDumpList.add("      Tier to      " + pad + "= <" + tmpRB.tierTo + ">");
              tmpDumpList.add("      Beat         " + pad + "= <" + tmpRB.beat + ">");
              tmpDumpList.add("      Unit price   " + pad + "= <" + tmpRB.factor + ">");
              tmpDumpList.add("      Charge base  " + pad + "= <" + tmpRB.chargeBase + ">");
              tmpDumpList.add("      Beats used   " + pad + "= <" + tmpRB.beatCount + ">");
              tmpDumpList.add("      RUM Rated    " + pad + "= <" + tmpRB.RUMRated + ">");
              tmpDumpList.add("      Rated Value  " + pad + "= <" + tmpRB.ratedAmount + ">");
              tmpDumpList.add("      Valid From   " + pad + "= <" + tmpRB.validFrom + ">");
              tmpDumpList.add("      ----------------");
            }
          } else {
            tmpDumpList.add("	----- NO RB -----");
          }
        }
      }
    }
    return tmpDumpList;
  }

 /**
  * Get the Balance Impact dump information for the balance impacts associated
  * with this record. Uses the default padding.
  *
  * @return Balance Impacts Dump Information
  */
  public ArrayList<String> getBalanceImpactsDump()
  {
    return getBalanceImpactsDump(24);
  }

 /**
  * Get the Balance Impact dump information for the balance impacts associated
  * with this record. Allows the padding to be defined.
  *
  * @param padding the number of spaces to pad to
  * @return Balance Impacts Dump Information
  */
  public ArrayList<String> getBalanceImpactsDump(int padding)
  {
    BalanceImpact tmpBalImp;

    // set up the padding
    String pad;
    if (padding < 19) {
      pad = "                                                  ".substring(1, 1);
    } else {
      pad = "                                                  ".substring(1, padding - 18);
    }

    ArrayList<String> tmpDumpList = new ArrayList<>();
    int tmpBalImpCount = this.getBalanceImpactCount();
    tmpDumpList.add("  Bal Impacts      " + pad + "= <" + tmpBalImpCount + ">");
    if (tmpBalImpCount>0)
    {
      tmpDumpList.add("------------ Balance Impacts --------------");
      for (int i = 0 ; i < tmpBalImpCount ; i++)
      {
        tmpBalImp = this.getBalanceImpact(i);
        if (tmpBalImp.type == null)
        {
          tmpDumpList.add("    ERROR: Null Balance Impact Type");
        }
        else
        {
          switch (tmpBalImp.type) {
            case "R":
              tmpDumpList.add("    Rating Impact");
              tmpDumpList.add("    Resource       " + pad + "= <" + tmpBalImp.Resource + ">");
              tmpDumpList.add("    Impact         " + pad + "= <" + tmpBalImp.balanceDelta + ">");
              tmpDumpList.add("    Balance Group  " + pad + "= <" + tmpBalImp.balanceGroup + ">");
              tmpDumpList.add("    Counter ID     " + pad + "= <" + tmpBalImp.counterID + ">");
              tmpDumpList.add("    Counter Rec ID " + pad + "= <" + tmpBalImp.recID + ">");
              tmpDumpList.add("    ----------------");
              break;
            case "D":
              tmpDumpList.add("    Discounting Impact");
              tmpDumpList.add("    Prod Name      " + pad + "= <" + tmpBalImp.cpiName + ">");
              tmpDumpList.add("    Rule Name      " + pad + "= <" + tmpBalImp.ruleName + ">");
              tmpDumpList.add("    Resource       " + pad + "= <" + tmpBalImp.Resource + ">");
              tmpDumpList.add("    RUM            " + pad + "= <" + tmpBalImp.rumUsed + ">");
              tmpDumpList.add("    Impact on RUM  " + pad + "= <" + tmpBalImp.rumValueUsed + ">");
              tmpDumpList.add("    RUM Value After" + pad + "= <" + tmpBalImp.rumValueAfter + ">");
              tmpDumpList.add("    Balance Group  " + pad + "= <" + tmpBalImp.balanceGroup + ">");
              tmpDumpList.add("    Counter ID     " + pad + "= <" + tmpBalImp.counterID + ">");
              tmpDumpList.add("    Counter Rec ID " + pad + "= <" + tmpBalImp.recID + ">");
              tmpDumpList.add("    Counter Delta  " + pad + "= <" + tmpBalImp.balanceDelta + ">");
              tmpDumpList.add("    Counter After  " + pad + "= <" + tmpBalImp.balanceAfter + ">");
              tmpDumpList.add("    Start Date     " + pad + "= <" + tmpBalImp.startDate + ">");
              tmpDumpList.add("    End Date       " + pad + "= <" + tmpBalImp.endDate + ">");
              tmpDumpList.add("    ----------------");
              break;
            default:
              tmpDumpList.add("    ERROR: Unknown Impact Type <" + tmpBalImp.type + ">");
              break;
          }
        }
      }
    }

    return tmpDumpList;
  }  
}
