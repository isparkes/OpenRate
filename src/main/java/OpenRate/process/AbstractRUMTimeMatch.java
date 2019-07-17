
package OpenRate.process;

import OpenRate.exception.ProcessingException;
import OpenRate.record.*;
import java.util.ArrayList;

/**
 * This class implements a time zoning and splitting module, based on the RUM
 * rating model. That means that Charge Packets are evaluated during the
 * processing and the results are generally written back to the Charge Packet.
 *
 * This module performs splitting in the following cases: - The Splitting Flag
 * in the Charge Packet has to be set to a value which indicates that splitting
 * is required: - 0 = no splitting - 1 = splitting equally of the duration over
 * the packets
 *
 * Splitting is done on the duration of the call, and the results of the
 * splitting operation are written into the "splittingFactor" field of the
 * charge packet. This double value indicates the fractional amount of the RUM
 * value which should be written into this charge packet.
 *
 * The duration of the call is taken from the RUMValue field, and this is
 * interpreted as the duration of the call. In cases where you do not want the
 * RUMValue interpreted as the duration, do NOT set the splitting flag. No
 * filtering is done on the RUM type, only the splitting flag is significant.
 *
 * This module can create many charge packets out of each 'seed' charge packet,
 * one for each zone that has been impacted. Zones of a similar type (e.g.
 * 'peak' or 'off-peak' are not aggregated, but instead are presented as
 * individual segments of validity, and should be rolled-up if necessary after
 * rating.
 *
 * Fields Read: - Charge Packet:timeModel - Charge Packet:Valid - Charge
 * Packet:timeSplitting - Charge Packet:priority - RatingRecord:EventDate
 *
 * Fields Written: - Charge Packet:timeResult - Charge Packet:timeSplitting -
 * Charge Packet:splittingFactor
 *
 */
public abstract class AbstractRUMTimeMatch extends AbstractTimeMatch {

  /**
   * Perform no splitting
   */
  public static final int TIME_SPLITTING_NO_CHECK = 0;

  /**
   * Perform splitting
   */
  public static final int TIME_SPLITTING_CHECK_SPLITTING = 1;

  /**
   * Perform splitting and take into account beats
   */
  public static final int TIME_SPLITTING_CHECK_SPLITTING_BEAT_ROUNDING = 2;

  /**
   * Perform no splitting - we are in a holiday
   */
  public static final int TIME_SPLITTING_HOLIDAY = -1;

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
  public void setExceptionReporting(boolean NewValue) {
    reportExceptions = NewValue;
  }

  /**
   * Calculate and write the rated amount into the record for detail records
   *
   * @param RecordToMatch The record we are matching on
   * @throws ProcessingException
   */
  protected void performRUMTimeMatch(RatingRecord RecordToMatch) throws ProcessingException {
    int Index;
    RecordError tmpError;
    ChargePacket tmpCP;
    boolean Errored = false;
    ArrayList<TimePacket> TimeZones;

    if (RecordToMatch instanceof RatingRecord) {
      RatingRecord CurrentRecord = RecordToMatch;

      // Check if we have the start date - we will need this in any case
      // Throw an exception if we do not have the date
      if (CurrentRecord.eventStartDate == null) {
        if (reportExceptions == false) {
          tmpError = new RecordError("ERR_EVENT_START_DATE_NOT_FOUND", ErrorType.DATA_NOT_FOUND, getSymbolicName());
          CurrentRecord.addError(tmpError);

          // our work is done here, get out
          return;
        } else {
          throw new ProcessingException("Event start date not found", getSymbolicName());
        }
      }

      // Cycle over the charge packets gathering the impacts as we go
      for (Index = 0; Index < CurrentRecord.getChargePacketCount(); Index++) {
        tmpCP = CurrentRecord.getChargePacket(Index);

        if (tmpCP.Valid) {
          switch (tmpCP.timeSplitting) {
            // It's a Holiday, do nothing - the holiday match should already
            // have done the work
            case TIME_SPLITTING_HOLIDAY: {
              break;
            }

            case TIME_SPLITTING_NO_CHECK: {
              try {
                // Use the time zoning from AbstractTimeMatch on this charge packet
                TimeZones = getTimeZone(tmpCP.timeModel, CurrentRecord.eventStartDate, CurrentRecord.eventStartDate);
                tmpCP.setTimeZones(TimeZones);

                if (TimeZones.size() == 1) {
                  // Reset the time splitting flag
                  tmpCP.timeSplitting = 0;
                } else if (TimeZones.isEmpty()) {
                  // No time zones found at all
                  if (reportExceptions == false) {
                    // We only want one error on the record
                    if ((!Errored) & (tmpCP.priority == 0)) {
                      tmpError = new RecordError("ERR_TIME_NOT_FOUND", ErrorType.SPECIAL, getSymbolicName());
                      CurrentRecord.addError(tmpError);
                      Errored = true;
                    }
                  } else {
                    throw new ProcessingException("Time zone not found", getSymbolicName());
                  }
                }
              } catch (Exception e) {
                if (reportExceptions == false) {
                  // Only error if this is a base packet
                  if (tmpCP.priority == 0) {
                    tmpError = new RecordError("ERR_TIME_MATCH_ERROR", ErrorType.SPECIAL, getSymbolicName(), e.getMessage());
                    CurrentRecord.addError(tmpError);
                  }
                } else {
                  throw new ProcessingException(e, getSymbolicName());
                }
              }
              break;
            }
            case TIME_SPLITTING_CHECK_SPLITTING:
            case TIME_SPLITTING_CHECK_SPLITTING_BEAT_ROUNDING: {
              try {
                // Check the end date, only if we need it for splitting
                if (CurrentRecord.eventEndDate == null) {
                  if (reportExceptions == false) {
                    tmpError = new RecordError("ERR_EVENT_END_DATE_NOT_FOUND", ErrorType.DATA_NOT_FOUND, getSymbolicName());
                    CurrentRecord.addError(tmpError);

                    // our work is done here, get out
                    return;
                  } else {
                    throw new ProcessingException("Event end date not found", getSymbolicName());
                  }
                }

                // Use the time zoning from AbstractTimeMatch on this charge packet
                TimeZones = getTimeZone(tmpCP.timeModel, CurrentRecord.eventStartDate, CurrentRecord.eventEndDate);
                tmpCP.setTimeZones(TimeZones);

                if (TimeZones.size() == 1) {
                  // Reset the time splitting flag
                  tmpCP.timeSplitting = 0;
                } else if (TimeZones.isEmpty()) {
                  // No time zones found at all
                  if (reportExceptions == false) {
                    // We only want one error on the record
                    if ((!Errored) & (tmpCP.priority == 0)) {
                      tmpError = new RecordError("ERR_TIME_NOT_FOUND", ErrorType.SPECIAL, getSymbolicName());
                      CurrentRecord.addError(tmpError);
                      Errored = true;
                    }
                  } else {
                    throw new ProcessingException("Time zone not found", getSymbolicName());
                  }
                }
              } catch (Exception e) {
                if (reportExceptions == false) {
                  // Only error if this is a base packet
                  if (tmpCP.priority == 0) {
                    tmpError = new RecordError("ERR_TIME_MATCH_ERROR", ErrorType.SPECIAL, getSymbolicName(), e.getMessage());
                    CurrentRecord.addError(tmpError);
                  }
                } else {
                  throw new ProcessingException(e, getSymbolicName());
                }
              }
              break;
            }
          }
        }
      }
    } else {
      if (reportExceptions == false) {
        tmpError = new RecordError("ERR_NOT_RATING_RECORD", ErrorType.SPECIAL, getSymbolicName());
        RecordToMatch.addError(tmpError);
      } else {
        throw new ProcessingException("Not a rating record", getSymbolicName());
      }
    }
  }
}
