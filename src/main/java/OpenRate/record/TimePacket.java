

package OpenRate.record;

/**
 * This holds the information about the various segments involved in a time match
 * in the case that we pass from one band to another.
 *
 * @author afzaal
 */
public class TimePacket
{
  /**
   * Used for numbering the packets
   */
  public int packetNumber;
  
 /**
  * The day of week of the segment
  */
  public int dayofWeek;

 /**
  * The start time (hours * 60 + minutes) of the segment
  */
  public int startTime;

 /**
  * The start second of the segment
  */
  public int startSecond;

 /**
  * The end time (hours * 60 + minutes) of the segment
  */
  public int endTime;

 /**
  * The end second of the segment
  */
  public int endSecond;

 /**
  * The duration (seconds) second of the segment
  */
  public int duration;

 /**
  * The total original duration of the event
  */
  public int totalDuration;

 /**
  * The time model applied to the segment
  */
  public String timeModel;

 /**
  * The time result found for the segment
  */
  public String timeResult;
  
 /**
  * The selected price model group
  */
  public String  priceGroup = null;

 /**
  * The selected price model
  */
  public String  priceModel = null;

  public TimePacket(TimePacket toClone) {
    this.packetNumber = toClone.packetNumber;
    this.dayofWeek = toClone.dayofWeek;
    this.duration = toClone.duration;
    this.startTime = toClone.startTime;
    this.startSecond = toClone.startSecond;
    this.endTime = toClone.endTime;
    this.endSecond = toClone.endSecond;
    this.timeModel = toClone.timeModel;
    this.timeResult = toClone.timeResult;
    this.totalDuration = toClone.totalDuration;
    this.priceGroup = toClone.priceGroup;
    this.priceModel = toClone.priceModel;
  }
  
  public TimePacket() {
  }
  
  /**
   * Create a clone of this charge packet
   *
   * @return The cloned Charge Packet
   */
  public TimePacket Clone() {
    return new TimePacket(this);
  }

}
