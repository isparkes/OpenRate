
package OpenRate.process;

import OpenRate.record.ChargePacket;
import OpenRate.record.RatingRecord;
import java.util.Calendar;

/**
 * This class performs a look up into the holiday calendar to see if the call
 * was made on a holiday. If so, the defined timeResult is set.
 *
 * @author Afzaal
 */
public abstract class AbstractRUMHolidayMatch
        extends AbstractRegexMatch {

  // this is used to check the dates

  private final String[] searchParameters = new String[3];
  Calendar calendar = Calendar.getInstance();

  /**
   * Check to see if the CDR start date is on a holiday day. If it is, set the
   * timeResult to the given value, and set the timeSplitting flag to NO_CHECK,
   * which means that the RUMTimeMatch module will not perform the normal check.
   *
   * The map group for the time match is read from the timeModel, meaning that
   * different holiday calendars can be mapped to different time models.
   *
   * The timeResult values are written into the charge packets.
   *
   * @param timeResult The result to write if the day is a holiday
   * @param recordToMatch The RatingRecord to work on
   */
  public void performCPHolidayMatch(RatingRecord recordToMatch, String timeResult) {
    int Index;
    ChargePacket tmpCP;

    calendar.setTime(recordToMatch.eventStartDate);
    searchParameters[0] = "" + calendar.get(Calendar.DAY_OF_MONTH);
    searchParameters[1] = "" + (calendar.get(Calendar.MONTH) + 1); // Incrementing by 1 as January = 0
    searchParameters[2] = "" + calendar.get(Calendar.YEAR);

    for (Index = 0; Index < recordToMatch.getChargePacketCount(); Index++) {
      tmpCP = recordToMatch.getChargePacket(Index);

      String RegexResult = getRegexMatch(tmpCP.timeModel, searchParameters);
      if (isValidRegexMatchResult(RegexResult)) {
        // set the flag to skip the time match
        tmpCP.timeSplitting = AbstractRUMTimeMatch.TIME_SPLITTING_HOLIDAY;
      }
    }
  }

  /**
   * Check to see if the CDR start date is on a holiday day. If it is, set the
   * timeResult to the given value, and set the timeSplitting flag to NO_CHECK,
   * which means that the RUMTimeMatch module will not perform the normal check.
   *
   * The map group for the time match is read from the MapGroup parameter,
   * meaning that a single holiday calendar is used for all time models.
   *
   * The timeResult values are written into ALL charge packets.
   *
   * @param recordToMatch The RatingRecord to work on
   * @param mapGroup The mapping group the use for the test
   * @param timeResult The result to write if the day is a holiday
   */
  public void performHolidayMatch(RatingRecord recordToMatch, String mapGroup, String timeResult) {
    int Index;
    ChargePacket tmpCP;

    calendar.setTime(recordToMatch.eventStartDate);
    searchParameters[0] = "" + calendar.get(Calendar.DAY_OF_MONTH);
    searchParameters[1] = "" + (calendar.get(Calendar.MONTH) + 1); // Incrementing by 1 as January = 0
    searchParameters[2] = "" + calendar.get(Calendar.YEAR);

    String RegexResult = getRegexMatch(mapGroup, searchParameters);
    if (isValidRegexMatchResult(RegexResult)) {
      for (Index = 0; Index < recordToMatch.getChargePacketCount(); Index++) {
        tmpCP = recordToMatch.getChargePacket(Index);

        // set the flag to skip the time match
        tmpCP.timeSplitting = AbstractRUMTimeMatch.TIME_SPLITTING_HOLIDAY;
      }
    }
  }
}
