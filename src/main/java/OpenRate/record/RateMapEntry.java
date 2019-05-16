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

