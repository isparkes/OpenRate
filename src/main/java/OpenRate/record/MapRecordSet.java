

package OpenRate.record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * The RecordSet is a group of Records. Currently it is preferred to use
 * Record Compression, as this offers higher performance. However, in some
 * situations it will be more suitable to create a map record set, which in
 * effect encapsulates multiple records inside a single record object.
 */
public class MapRecordSet extends AbstractRecord
{
  private static final long serialVersionUID = -4616651522453963995L;

  private Map<Object, IRecord>    records;
  private Object rootRecord; // Source IRecord for the record set

  /**
   * Constructor
   */
  public MapRecordSet()
  {
    this.records = new HashMap<>();
  }

  /**
   * Constructor
   *
   * @param source The source key that we are going to use to identify this
   * record set
   */
  public MapRecordSet(Object source)
  {
    this.records   = new HashMap<>();
    this.rootRecord    = source;
  }

  /**
   * returns the Source Key
   *
   * @return The source key for this record
   */
  public Object getSourceKey()
  {
    return rootRecord;
  }

  /**
   * Add a new IRecord to the set. New Records are appended to the
   * end of the Collection
   *
   * @param key The record key
   * @param r The record
   */
  public void put(Object key, IRecord r)
  {
    records.put(key, r);
  }

  /**
   * get a IRecord from the set for the given key
   *
   * @param key The record key
   * @return The record
   */
  public Object get(Object key)
  {
    return records.get(key);
  }

  @Override
    public ArrayList<String> getDumpInfo() {
        return null;
    }
}
