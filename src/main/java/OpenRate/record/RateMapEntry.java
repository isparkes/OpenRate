

package OpenRate.record;

/**
 * Rate Plan entry. A rate plan is made up of a list of these. Each of the
 * individual entries is a linked list with a validity period. During the
 * loading, the list is ordered by start date, and traversed during the
 * rating phase to find the right validity segment for rating.
 * 
 * Each step in the plan is valid from the start time until the next start time
 *
 * @author ian
 */
public class RateMapEntry
{
  private int    step;
  private double from;
  private double to;
  private double beat;
  private double factor;
  private double chargeBase;
  private long  startTime;
  private RateMapEntry child = null;

  /**
   * @return the step
   */
  public int getStep() {
    return step;
  }

  /**
   * @param Step the step to set
   */
  public void setStep(int Step) {
    this.step = Step;
  }

  /**
   * @return the from
   */
  public double getFrom() {
    return from;
  }

  /**
   * @param From the from to set
   */
  public void setFrom(double From) {
    this.from = From;
  }

  /**
   * @return the to
   */
  public double getTo() {
    return to;
  }

  /**
   * @param To the to to set
   */
  public void setTo(double To) {
    this.to = To;
  }

  /**
   * @return the beat
   */
  public double getBeat() {
    return beat;
  }

  /**
   * @param Beat the beat to set
   */
  public void setBeat(double Beat) {
    this.beat = Beat;
  }

  /**
   * @return the factor
   */
  public double getFactor() {
    return factor;
  }

  /**
   * @param Factor the factor to set
   */
  public void setFactor(double Factor) {
    this.factor = Factor;
  }

  /**
   * @return the chargeBase
   */
  public double getChargeBase() {
    return chargeBase;
  }

  /**
   * @param ChargeBase the chargeBase to set
   */
  public void setChargeBase(double ChargeBase) {
    this.chargeBase = ChargeBase;
  }

  /**
   * @return the startTime
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * @param StartTime the startTime to set
   */
  public void setStartTime(long StartTime) {
    this.startTime = StartTime;
  }

  /**
   * @return the child
   */
  public RateMapEntry getChild() {
    return child;
  }

  /**
   * @param child the child to set
   */
  public void setChild(RateMapEntry child) {
    this.child = child;
  }
}

