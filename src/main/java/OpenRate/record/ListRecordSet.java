

package OpenRate.record;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The RecordSet is a group of Records.
 */
public class ListRecordSet extends AbstractRecord
{
  private static final long serialVersionUID = -8721041925347090968L;

  private List<IRecord>   records;
  private Object source; // Source IRecord for the record set

  /**
   * Constructor
   */
  public ListRecordSet()
  {
    this.records = new ArrayList<>();
  }

  /**
   * Constructor
   *
   * @param source The source record
   */
  public ListRecordSet(Object source)
  {
    this.records   = new ArrayList<>();
    this.source    = source;
  }

  /**
   * returns the Source Key
   *
   * @return The source key
   */
  public Object getSourceKey()
  {
    return source;
  }

  /**
   * Add a new IRecord to the set. New Records are appended to the
   * end of the Collection
   *
   * @param r The record to add to the set
   */
  public void add(IRecord r)
  {
    records.add(r);
  }

  /**
   * Remove a IRecord from the set.
   *
   * @param r The record to remove
   * @return True if it was removed
   */
  public boolean remove(IRecord r)
  {
    return records.remove(r);
  }

  /**
   * Return the Iterator of the List
   *
   * @return The iterator
   */
  public Iterator<IRecord> iterator()
  {
    return records.iterator();
  }

  @Override
    public ArrayList<String> getDumpInfo() {
        return null;
    }
}
