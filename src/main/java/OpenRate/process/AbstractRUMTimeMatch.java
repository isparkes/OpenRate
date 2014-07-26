/* ====================================================================
 * Limited Evaluation License:
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
package OpenRate.process;

import OpenRate.exception.ProcessingException;
import OpenRate.record.*;
import java.util.ArrayList;

/**
 * This class implements a time zoning and splitting module, based on the RUM
 * rating model. That means that Charge Packets are evaluated during the
 * processing and the results are generally written back to the Charge
 * Packet.
 *
 * This module performs splitting in the following cases:
 *  - The Splitting Flag in the Charge Packet has to be set to a value which
 *    indicates that splitting is required:
 *    - 0 = no splitting
 *    - 1 = splitting equally of the duration over the packets
 *
 * Splitting is done on the duration of the call, and the results of the
 * splitting operation are written into the "splittingFactor" field of the
 * charge packet. This double value indicates the fractional amount of the RUM
 * value which should be written into this charge packet.
 *
 * The duration of the call is taken from the RUMValue field, and this is
 * interpreted as the duration of the call. In cases where you do not want
 * the RUMValue interpreted as the duration, do NOT set the splitting flag. No
 * filtering is done on the RUM type, only the splitting flag is significant.
 *
 * This module can create many charge packets out of each 'seed' charge packet,
 * one for each zone that has been impacted. Zones of a similar type (e.g.
 * 'peak' or 'off-peak' are not aggregted, but instead are presented as
 * individual segements of validity, and should be rolled-up if necessary after
 * rating.
 *
 * Fields Read:
 *  - Charge Packet:timeModel
 *  - Charge Packet:Valid
 *  - Charge Packet:timeSplitting
 *  - Charge Packet:priority
 *  - RatingRecord:EventDate
 *
 * Fields Written:
 *  - Charge Packet:timeResult
 *  - Charge Packet:timeSplitting
 *  - Charge Packet:splittingFactor
 *
 */
public abstract class AbstractRUMTimeMatch extends AbstractTimeMatch
{
  /**
   * Perform no splitting
   */
  public static final int TIME_SPLITTING_NO_CHECK        = 0;

  /**
   * Perform splitting
   */
  public static final int TIME_SPLITTING_CHECK_SPLITTING = 1;

  /**
   * Perform no splitting - we are in a holiday
   */
  public static final int TIME_SPLITTING_HOLIDAY         = -1;

  // this tells us whether to deal with the exception ourself, pr pass it to the
  // parent module for handling
  private boolean reportExceptions = false;

  // -----------------------------------------------------------------------------
  // ------------------------ Start of custom functions --------------------------
  // -----------------------------------------------------------------------------

 /**
  * Set the state of the exception reporting. True means that we let the parent
  * module deal with it, false means that we deal with it ourselves.
  *
  * @param NewValue
  */
  public void setExceptionReporting(boolean NewValue)
  {
    reportExceptions = NewValue;
  }

 /**
  * Calculate and write the rated amount into the record for detail records
  *
  * @param RecordToMatch The record we are matching on
  * @throws ProcessingException
  */
  protected void performRUMTimeMatch(RatingRecord RecordToMatch) throws ProcessingException
  {
    int          Index;
    int          ZoneIndex;
    String       TimeZone;
    RecordError  tmpError;
    ChargePacket tmpCP;
    ChargePacket tmpCPCloneSource = null;
    boolean      Errored = false;
    ArrayList<TimePacket> TimeZones;
    ChargePacket tmpCPNew;
    ArrayList<ChargePacket> tmpCPList = new ArrayList<>();

    if (RecordToMatch instanceof RatingRecord)
    {
      RatingRecord CurrentRecord = (RatingRecord) RecordToMatch;

      // Check if we have the start date - we will need this in any case
      // Throw an exception if we do not have the date
      if (CurrentRecord.EventStartDate == null)
      {
        if (reportExceptions == false)
        {
          tmpError = new RecordError("ERR_EVENT_START_DATE_NOT_FOUND", ErrorType.DATA_NOT_FOUND, getSymbolicName());
          CurrentRecord.addError(tmpError);

          // our work is done here, get out
          return;
        }
        else
        {
          throw new ProcessingException("Event start date not found",getSymbolicName());
        }
      }

      // Cycle over the charge packets gathering the impacts as we go
      for (Index = 0 ; Index < CurrentRecord.getChargePacketCount() ; Index++)
      {
        tmpCP = CurrentRecord.getChargePacket(Index);

        if (tmpCP.Valid)
        {
          switch (tmpCP.timeSplitting)
          {
            // It's a Holiday, do nothing - the holiday match shoudl already
            // have done the work
          	case TIME_SPLITTING_HOLIDAY:
          	{
          		break;
          	}

            // We do not need to split - base the time zone on the start date
            case TIME_SPLITTING_NO_CHECK:
            {
              try
              {
                // Use the time zoning from AbstractTimeMatch on this charge packet
                TimeZone = getTimeZone(tmpCP.timeModel,CurrentRecord.EventStartDate);

                if (isValidTimeMatchResult(TimeZone) == false)
                {
                  if (reportExceptions == false)
                  {
                    // We only want one error on the record
                    if ((!Errored) & (tmpCP.priority == 0))
                    {
                      tmpError = new RecordError("ERR_TIME_NOT_FOUND", ErrorType.SPECIAL, getSymbolicName());
                      CurrentRecord.addError(tmpError);
                      Errored = true;
                    }
                  }
                  else
                  {
                    throw new ProcessingException("Time zone not found",getSymbolicName());
                  }
                }

                // Write the information back into the record
                fillCPWithTimeMatchChildData(CurrentRecord,tmpCP,TimeZone,new Double(1));
              }
              catch (Exception e)
              {
                if (reportExceptions == false)
                {
                  // Only error if this is a base packet
                  if (tmpCP.priority == 0)
                  {
                    tmpError = new RecordError("ERR_TIME_MATCH_ERROR", ErrorType.SPECIAL, getSymbolicName(), e.getMessage());
                    CurrentRecord.addError(tmpError);
                  }
                }
                else
                {
                  throw new ProcessingException(e,getSymbolicName());
                }
              }

              break;
            }

            // perform splitting - we need to split the event over the
            // Period Start Date -> End date
            case TIME_SPLITTING_CHECK_SPLITTING:
            {
              try
              {
                // Check the end date, only if we need it for splitting
                if (CurrentRecord.EventEndDate == null)
                {
                  if (reportExceptions == false)
                  {
                    tmpError = new RecordError("ERR_EVENT_END_DATE_NOT_FOUND", ErrorType.DATA_NOT_FOUND, getSymbolicName());
                    CurrentRecord.addError(tmpError);

                    // our work is done here, get out
                    return;
                  }
                  else
                  {
                    throw new ProcessingException("Event end date not found",getSymbolicName());
                  }
                }

                // Use the time zoning from AbstractTimeMatch on this charge packet
                TimeZones = getTimeZone(tmpCP.timeModel,CurrentRecord.EventStartDate,CurrentRecord.EventEndDate);

                if (TimeZones.size() == 1)
                {

                  // Deal with the case that there is no match
                  if (TimeZones.get(0).TimeResult.equals("NOMATCH"))
                  {
                    if (reportExceptions == false)
                    {
                      // We only want one error on the record
                      if ((!Errored) & (tmpCP.priority == 0))
                      {
                        tmpError = new RecordError("ERR_TIME_NOT_FOUND", ErrorType.SPECIAL, getSymbolicName());
                        CurrentRecord.addError(tmpError);
                        Errored = true;
                      }
                    }
                    else
                    {
                      throw new ProcessingException("Time zone not found",getSymbolicName());
                    }
                  }
                  else
                  {
                    // Write the result information back into the record
                    fillCPWithTimeMatchChildData(CurrentRecord,tmpCP,TimeZones.get(0).TimeResult,new Double(1));
                  }

                  // Reset the time splitting flag
                  tmpCP.timeSplitting = 0;
                }
                else if (TimeZones.size() > 1)
                {
                  // multiple results, loop through them
                  for (ZoneIndex = 0 ; ZoneIndex < TimeZones.size() ; ZoneIndex++)
                  {
                    if (ZoneIndex == 0)
                    {
                      // re-use the existing charge packet
                      fillCPWithTimeMatchChildData(CurrentRecord,tmpCP,TimeZones.get(ZoneIndex).TimeResult,TimeZones.get(ZoneIndex).Duration / (double) TimeZones.get(ZoneIndex).TotalDuration);

                      // save the packet we are going to be cloning
                      tmpCPCloneSource = tmpCP;
                    }
                    else
                    {
                      // other charge packets need to be cloned
                      tmpCPNew = tmpCPCloneSource.Clone();

                      // update the information in the packet
                      fillCPWithTimeMatchChildData(CurrentRecord,tmpCP,TimeZones.get(ZoneIndex).TimeResult,TimeZones.get(ZoneIndex).Duration / (double) TimeZones.get(ZoneIndex).TotalDuration);

                      // buffer the new charge packet
                      tmpCPList.add(tmpCPNew);
                    }
                  }
                }
                else
                {
                  // No time zones found at all
                  if (reportExceptions == false)
                  {
                    // We only want one error on the record
                    if ((!Errored) & (tmpCP.priority == 0))
                    {
                      tmpError = new RecordError("ERR_TIME_NOT_FOUND", ErrorType.SPECIAL, getSymbolicName());
                      CurrentRecord.addError(tmpError);
                      Errored = true;
                    }
                  }
                  else
                  {
                    throw new ProcessingException("Time zone not found",getSymbolicName());
                  }
                }
              }
              catch (Exception e)
              {
                if (reportExceptions == false)
                {
                  // Only error if this is a base packet
                  if (tmpCP.priority == 0)
                  {
                    tmpError = new RecordError("ERR_TIME_MATCH_ERROR", ErrorType.SPECIAL, getSymbolicName(), e.getMessage());
                    CurrentRecord.addError(tmpError);
                  }
                }
                else
                {
                  throw new ProcessingException(e,getSymbolicName());
                }
              }

              break;
            }
          }
        }
      }

      // add the buffered charge packets
      for (Index = 0 ; Index < tmpCPList.size() ; Index++)
      {
        CurrentRecord.addChargePacket(tmpCPList.get(Index));
      }
    }
    else
    {
      if (reportExceptions == false)
      {
        tmpError = new RecordError("ERR_NOT_RATING_RECORD", ErrorType.SPECIAL, getSymbolicName());
        RecordToMatch.addError(tmpError);
      }
      else
      {
        throw new ProcessingException("Not a rating record",getSymbolicName());
      }
    }
  }

 /**
  * This method is intended to be overwritten in the case that the standard
  * best match processing is to be changed. By default, it fills the zoneResult
  * field with the first parameter that is recovered and the zoneInfo with the
  * second.
  *
  * @param Record The record we are to fill
  * @param tmpCP The Charge Packet we are working on
   * @param TimeZone The time zone
   * @param splittingFactor the splitting factor, i.e. how much of the period was in this time zone
  */
  protected void fillCPWithTimeMatchChildData(RatingRecord Record, ChargePacket tmpCP, String TimeZone, Double splittingFactor)
  {
    tmpCP.timeResult = TimeZone;
    tmpCP.splittingFactor = splittingFactor;
  }
}
