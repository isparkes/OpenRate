

package OpenRate.buffer;

/**
 * BufferEvent is the class that is used to notify monitors that something
 * has happened that they should be aware of. At the moment, the only event
 * that they should be aware of is the arrival of new records to be processed.
 */
public class BufferEvent
  implements IEvent
{
  // This holds the type of event that we are representing
  private String            type = null;

 /**
  * Event corresponding to new channel records.
  */
  public static BufferEvent NEW_RECORDS = new BufferEvent("NEW_RECORDS");

 /**
  * Default Constructor for BufferEvent
  */
  private BufferEvent()
  {
    type = "NONE";
  }

 /**
  * Constructor for BufferEvent, with type given
  *
  * @param type The type of event that is being created
  */
  private BufferEvent(String type)
  {
    this.type = type;
  }

 /**
  * Provides readable representation of the object
  *
  * @return the readable Buffer Event Type
  */
  @Override
  public String toString()
  {
    return "BufferEvent: type = " + type;
  }
}
