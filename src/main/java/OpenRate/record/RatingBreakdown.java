
package OpenRate.record;

/**
 * Defines the rating result that details the way that the rating was performed
 * so that it can be exposed in the record.
 *
 * @author ian
 */
public class RatingBreakdown {

  /**
   * Price model step that was used to rate this
   */
  public int stepUsed;

  /**
   * Start of step tier
   */
  public double tierFrom;

  /**
   * End of step tier
   */
  public double tierTo;

  /**
   * How many of the RUM that was rated with this
   */
  public double RUMRated;

  /**
   * The price factor for this step
   */
  public double factor;

  /**
   * The beat of this step
   */
  public double beat;

  /**
   * The charge base of this step
   */
  public double chargeBase;

  /**
   * The rated value of this step
   */
  public double ratedAmount;

  /**
   * How many beats were rated in this step
   */
  public long beatCount;

  /**
   * Validity from of the step
   */
  public long validFrom;

  public RatingBreakdown(RatingBreakdown toCloneRB) {
    this.beat = toCloneRB.beat;
    this.beatCount = toCloneRB.beatCount;
    this.chargeBase = toCloneRB.chargeBase;
    this.factor = toCloneRB.factor;
    this.ratedAmount = toCloneRB.ratedAmount;
    this.RUMRated = toCloneRB.RUMRated;
    this.stepUsed = toCloneRB.stepUsed;
    this.tierFrom = toCloneRB.tierFrom;
    this.tierTo = toCloneRB.tierTo;
    this.validFrom = toCloneRB.validFrom;
  }

  public RatingBreakdown() {
  }

  /**
   * Create a clone of this charge packet
   *
   * @return The cloned Charge Packet
   */
  public RatingBreakdown Clone() {
    return new RatingBreakdown(this);
  }

}
