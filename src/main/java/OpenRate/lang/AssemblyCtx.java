package OpenRate.lang;

/**
 * This class is used to track the assembly of voice or data partials, and allows
 * persistence on system startup or shutdown. It manages the accumulated totals
 * we well as the state of the context.
 *
 * @author ian
 */
public class AssemblyCtx
{
  /**
   * the cumulated duration
   */
  public double totalDuration = 0;

  /**
   * the cumulated volume (uplink and downlink)
   */
  public double totalData = 0;

  /**
   * the cumulated uplink
   */
  public double uplink = 0;

  /**
   * the cumulated downlink
   */
  public double downlink = 0;

  /**
   * Ctx state: 1 = start, 2 = intermediate, 3 = closed
   */
  public int    state = 0;

  /**
   * the start date of the first partial
   */
  public long   StartDate = 0;

  /**
   * the date the context was closed, for purging
   */
  public long   ClosedDate = 0;
}
