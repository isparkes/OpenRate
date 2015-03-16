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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A Charge Packet holds the rating information to drive the zoning and rating.
 * Each time a change in the rating happens, a new charge packet is needed.
 *
 * @author ian
 */
public class ChargePacket {

  /**
   * Identifier for Flat Rating
   */
  public static final int RATING_TYPE_FLAT = 1;

  /**
   * Identifier for Tiered Rating
   */
  public static final int RATING_TYPE_TIERED = 2;

  /**
   * Identifier for Threshold Rating
   */
  public static final int RATING_TYPE_THRESHOLD = 3;

  /**
   * Identifier for Event Rating
   */
  public static final int RATING_TYPE_EVENT = 4;

  /**
   * The packet is valid until it is invalidated
   */
  public boolean Valid = true;

  /**
   * Type of packet, value depends on scenario
   */
  public String packetType = null;

  /**
   * Name of the rate plan we are rating
   */
  public String ratePlanName = null;

  /**
   * The zone model that we are using
   */
  public String zoneModel = null;

  /**
   * The time model we are using
   */
  public String timeModel = null;

  /**
   * The name of the service
   */
  public String service = null;

  /**
   * The name of the RUM used in this charge packet
   */
  public String rumName;

  /**
   * The amount of RUM rated in this packet
   */
  public double rumQuantity = 0;

  /**
   * The name of the resource to impact
   */
  public String resource = null;

  /**
   * The counter ID of the resource to impact
   */
  public int resCounter = 0;

  /**
   * The calculated value for this packet
   */
  public double chargedValue = 0;

  /**
   * 0 are base products, > 0 are override products
   */
  public int priority = 0;

  /**
   * The result of the zoning
   */
  public String zoneResult = null;

  /**
   * Description of the zone result
   */
  public String zoneInfo;

  /**
   * The ID of the subscription used
   */
  public String subscriptionID;

  /**
   * The rating type to apply for this packet
   */
  public int ratingType;

  /**
   * Human readable rating type
   */
  public String ratingTypeDesc;

  /**
   * tiemSplitting is used to control whether the time zoning module splits up
   * charge on the basis of the duration of an event.
   *
   * 0 = no splitting (Default) 1 = splitting
   */
  public int timeSplitting = 0;

  /**
   * The rating breakdown tells us about calculation that we performed at each
   * step of the rating.
   */
  public List<RatingBreakdown> breakDown;

  // Time zones that we are using
  private List<TimePacket> TimeZones;
  
  // Whether we are to consume the RUM or not
  public boolean consumeRUM;

  /**
   * Creates a new instance of ChargePacket
   */
  public ChargePacket() {
  }

  /**
   * Create a clone of a charge packet
   *
   * @param toClone
   * @param deep
   */
  public ChargePacket(ChargePacket toClone,boolean deep) {
    this.packetType = toClone.packetType;
    this.ratePlanName = toClone.ratePlanName;
    this.zoneModel = toClone.zoneModel;
    this.timeModel = toClone.timeModel;
    this.service = toClone.service;
    this.rumName = toClone.rumName;
    this.rumQuantity = toClone.rumQuantity;
    this.resource = toClone.resource;
    this.chargedValue = toClone.chargedValue;
    this.priority = toClone.priority;
    this.zoneResult = toClone.zoneResult;
    this.zoneInfo = toClone.zoneInfo;
    this.subscriptionID = toClone.subscriptionID;
    this.ratingType = toClone.ratingType;
    this.ratingTypeDesc = toClone.ratingTypeDesc;
    this.timeSplitting = toClone.timeSplitting;
    this.consumeRUM = toClone.consumeRUM;
    this.TimeZones = new ArrayList<>();
    this.breakDown = new ArrayList<>();

    if (deep) {
      if (toClone.TimeZones != null) {
        Iterator<TimePacket> tpIter = toClone.TimeZones.iterator();
        while (tpIter.hasNext()) {
          TimePacket toCloneTP = tpIter.next();
          this.TimeZones.add(new TimePacket(toCloneTP));
        }
      }

      // in the case that we have a rating breakdown, clone that too
      if (toClone.breakDown != null) {
        Iterator<RatingBreakdown> bdIter = toClone.breakDown.iterator();
        while (bdIter.hasNext()) {
          RatingBreakdown toCloneRB = bdIter.next();
          this.breakDown.add(new RatingBreakdown(toCloneRB));
        }
      }
    } else {
    }
  }

  /**
   * Create a clone of this charge packet, copying all Time Packet and 
   * Rating Breakdown information.
   *
   * @return The cloned Charge Packet
   */
  public ChargePacket deepClone() {
    return new ChargePacket(this,true);
  }

  /**
   * Create a clone of this charge packet, excluding all Time Packet and 
   * Rating Breakdown information.
   *
   * @return The cloned Charge Packet
   */
  public ChargePacket shallowClone() {
    return new ChargePacket(this,false);
  }

  /**
   * @return the TimeZones
   */
  public List<TimePacket> getTimeZones() {
    return TimeZones;
  }

  /**
   * @param TimeZones the TimeZones to set
   */
  public void setTimeZones(List<TimePacket> TimeZones) {
    this.TimeZones = TimeZones;
  }
  
  /**
   * Add a TimePacket to the TimePacket list. If the list is not yet set up,
   * set it up.
   * 
   * @param tmpTZ 
   */
  public void addTimeZone(TimePacket tmpTZ) {
    if (this.TimeZones == null) {
      this.TimeZones = new ArrayList<>();
    }
    this.TimeZones.add(tmpTZ);
  }
  
  public void addBreakdown(ArrayList<RatingBreakdown> newBreakdownList) {
    if (newBreakdownList == null) {
      // well, we're not going to add nothing to the list, so just get out
      return;
    }
    
    if (breakDown == null) {
      breakDown = newBreakdownList;
    } else {
      breakDown.addAll(newBreakdownList);
    }
  }
}
