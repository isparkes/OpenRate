

package OpenRate.buffer;


/**
 * Definition of the monitor infrastructure, which triggers a notification once
 * records have been accepted by a buffer for passing on. This means that
 * the downstream module will accept the records at the earliest moment
 * possible.
 */
public interface IMonitor
{
 /**
  * Perform the notification.
  *
  * @param e The event to notify
  */
  public void notify(IEvent e);
}
