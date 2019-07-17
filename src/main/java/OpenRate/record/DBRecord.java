

package OpenRate.record;

import java.util.ArrayList;
import java.util.Date;

/**
 * DB record is the most basic way of storing the column data that we get back
 * from the DB input adapter. It is not a sensible idea to put all the information
 * together as we do in the FlatRecord, as we are only going to have to take
 * it all apart again before we can work with it, thus the DB record is a way of
 * keeping anonymised column data.
 *
 * The columns as specified in the JDBCInputAdapter select statement will be
 * placed into the OriginalColumns array.
 *
 * The values in the OutputColumns will be written by the JDBCOutputAdapter
 * during writing.
 */
public class DBRecord extends AbstractRecord
{
  private static final long serialVersionUID = -4090030392623587158L;

  private String[] OriginalColumns;
  private Object[] OutputColumns;
  private int[]    OutputColumnTypes;
  private int      ColumnCount;
  private int      OutputColumnCount;

  /**
   * Defintion of column type as String
   */
  public static final int COL_TYPE_STRING  = 0;

  /**
   * Defintion of column type as Integer
   */
  public static final int COL_TYPE_INTEGER = 1;

  /**
   * Defintion of column type as Double
   */
  public static final int COL_TYPE_DOUBLE  = 2;

  /**
   * Defintion of column type as Long
   */
  public static final int COL_TYPE_LONG    = 3;

  /**
   * Defintion of column type as Date
   */
  public static final int COL_TYPE_DATE    = 4;

  /**
   * Defintion of column type as Boolean
   */
  public static final int COL_TYPE_BOOL    = 5;

  /**
   * Defintion of column type as Time
   */
  public static final int COL_TYPE_TIME    = 6;

  /**
   * Defintion of column type as Binary
   */
  public static final int COL_TYPE_BINARY  = 7;

 /**
  * Creates a new instance of DBRecord
  *
  * @param ColumnCount The total number of columns we expect to fill
  * @param Columns The column data
  * @param RecordNumber The record number
  */
  public DBRecord(int ColumnCount, String[] Columns, int RecordNumber)
  {
    super();

    this.recordNumber      = RecordNumber;
    this.ColumnCount       = ColumnCount;
    this.OriginalColumns   = new String[ColumnCount];
    this.OriginalColumns   = Columns;
  }

  /** Creates a new instance of DBRecord */
  public DBRecord()
  {
    super();
  }

 /**
  * Get all of the columns in the input record in string format
  *
  * @return The original raw input columns
  */
  public String[] getOriginalColumns()
  {
    return this.OriginalColumns;
  }

 /**
  * Get the number of columns that have been set
  *
  * @return The number of columns
  */
  public int getColumnCount()
  {
      return this.ColumnCount;
  }

 /**
  * Return the value of the column that we hav defined, as a string value
  *
  * @param Index The column number that is requested
  * @return The value of the column, as a string
  */
  public String getColumnValueString(int Index)
  {
      return this.OriginalColumns[Index];
  }

 /**
  * Set the output to have the list of columns passed as the input. This sets
  * all columns to use the default string type.
  *
  * @param Columns the number of columns we are setting
  * @param Data The data that we should set in the columns
  */
  public void setOutputData(int Columns, String[] Data)
  {
    int i;

    this.OutputColumnCount = Columns;
    this.OutputColumns = new Object[Columns];
    this.OutputColumns = Data;
    this.OutputColumnTypes = new int[Columns];
    for (i = 0 ;  i < Columns ; i++)
    {
      this.OutputColumnTypes[i] = 0; // string
    }
  }

 /**
  * Set the number of output columns we are using. The values then have to
  * be set using the "setOutputColumnString", "setOutputColumnInt" etc
  * methods. The number of columns should match up with the number of bind
  * parameter variables in the SQL statement. OpenRate expects to receive one
  * column variable per bind parameter.
  *
  * @param Columns The number of columns to set
  */
  public void setOutputColumnCount(int Columns)
  {
    int i;

    this.OutputColumnCount = Columns;
    this.OutputColumns = new Object[Columns];
    this.OutputColumnTypes = new int[Columns];

    for (i = 0 ;  i < Columns ; i++)
    {
      this.OutputColumnTypes[i] = 0; // string
    }
  }

 /**
  * Set the output column as a string format. Set value of a single output 
  * column. Set the number of columns to put into the output using 
  * "setOutputColumnCount".
  *
  * @param Column The column number to set, first parameter has index 1
  * @param Data The value to set
  */
  public void setOutputColumnString(int Column, String Data)
  {
    this.OutputColumns[Column] = Data;
    this.OutputColumnTypes[Column] = COL_TYPE_STRING;
  }

 /**
  * Set the output column as a integer format. Set value of a single output 
  * column. Set the number of columns to put into the output using 
  * "setOutputColumnCount".
  *
  * @param Column The column number to set, first parameter has index 1
  * @param Value The value to set
  */
  public void setOutputColumnInt(int Column, Integer Value)
  {
    this.OutputColumns[Column] = Value;
    this.OutputColumnTypes[Column] = COL_TYPE_INTEGER;
  }

 /**
  * Set the output column as a double format. Set value of a single output 
  * column. Set the number of columns to put into the output using 
  * "setOutputColumnCount".
  *
  * @param Column The column number to set, first parameter has index 1
  * @param Value The value to set
  */
  public void setOutputColumnDouble(int Column, Double Value)
  {
    this.OutputColumns[Column] = Value;
    this.OutputColumnTypes[Column] = COL_TYPE_DOUBLE;
  }

 /**
  * Set the output column as a long format. Set value of a single output 
  * column. Set the number of columns to put into the output using 
  * "setOutputColumnCount".
  *
  * @param Column The column number to set, first parameter has index 1
  * @param Value The value to set
  */
  public void setOutputColumnLong(int Column, Long Value)
  {
    this.OutputColumns[Column] = Value;
    this.OutputColumnTypes[Column] = COL_TYPE_LONG;
  }

 /**
  * Set the output column as a date format, using a date as an input. Set value 
  * of a single output column. Set the number of columns to put into the output
  * using "setOutputColumnCount".
  *
  * @param Column The column number to set, first parameter has index 1
  * @param Value The value to set
  */
  public void setOutputColumnDate(int Column, Date Value)
  {
    this.OutputColumns[Column] = Value.getTime();
    this.OutputColumnTypes[Column] = COL_TYPE_DATE;
  }

 /**
  * Set the output column as a date format, using a long as an input. Set value 
  * of a single output column. Set the number of columns to put into the output 
  * using "setOutputColumnCount".
  *
  * @param Column The column number to set, first parameter has index 1
  * @param Value The value to set
  */
  public void setOutputColumnDate(int Column, Long Value)
  {
    this.OutputColumns[Column] = Value;
    this.OutputColumnTypes[Column] = COL_TYPE_DATE;
  }

 /**
  * Set the output column as a boolean format. Set value of a single output 
  * column. Set the number of columns to put into the output using 
  * "setOutputColumnCount".
  *
  * @param Column The column number to set, first parameter has index 1
  * @param Value The value to set
  */
  public void setOutputColumnBool(int Column, Boolean Value)
  {
    if ((Boolean)Value)
    {
      this.OutputColumns[Column] = "1";
    }
    else
    {
      this.OutputColumns[Column] = "0";
    }
    this.OutputColumnTypes[Column] = COL_TYPE_BOOL;
  }

 /**
  * Set the output column as a time format. Set value of a single output 
  * column. Set the number of columns to put into the output using 
  * "setOutputColumnCount".
  *
  * @param Column The column number to set, first parameter has index 1
  * @param Value The value to set
  */
  public void setOutputColumnTime(int Column, Date Value)
  {
    this.OutputColumns[Column] = Value;
    this.OutputColumnTypes[Column] = COL_TYPE_TIME;
  }

 /**
  * Set the output column as bytes. Set value of a single output 
  * column. Set the number of columns to put into the output using 
  * "setOutputColumnCount".
  *
  * @param Column The column number to set, first parameter has index 1
  * @param Value The value to set
  */
  public void setOutputColumnBytes(int Column, byte[] Value)
  {
	  this.OutputColumns[Column] = Value ;
	  this.OutputColumnTypes[Column] = COL_TYPE_BINARY;
  }

 /**
  * Basic dumping strategy, usually to be overwritten in an implementation
  * of this class
  *
  * @return The dump information
  */
  @Override
  public ArrayList<String> getDumpInfo()
  {
    ArrayList<String> tmpDumpList;
    int i;
    tmpDumpList = new ArrayList<>();

    tmpDumpList.add("============ BEGIN RECORD ============");
    for ( i = 0 ; i < OutputColumns.length ; i++)
    {
      tmpDumpList.add("  FIELD <" + i + "> Value <" + OutputColumns[i] + ">");
    }

    return tmpDumpList;
  }

 /**
  * Get all of the output columns as string values
  *
  * @return The array of the raw column values
  */
  public Object[] getOutputColumns()
  {
    return  this.OutputColumns;
  }
 /**
  * Get the number of columns in the record
  *
  * @return The number of columns
  */
  public int getOutputColumnCount()
  {
    return this.OutputColumnCount;
  }

 /**
  * Get the output column value as a string
  *
  * @param Index The column index to retrieve the data for
  * @return The column value as a string
  */
  public String getOutputColumnValueString(int Index)
  {
    return (String)this.OutputColumns[Index];
  }

 /**
  * Set the output column as bytes
  *
  * @param Index The column index to retrieve the data for
  * @return The column value as bytes
  */
  public byte[] getOutputColumnValueBytes(int Index)
  {
	  return (byte[])this.OutputColumns[Index];
  }

 /**
  * Get the output column value as an integer
  *
  * @param Index The column index to retrieve the data for
  * @return The column value as an integer
  */
  public Integer getOutputColumnValueInt(int Index)
  {
    return (Integer)this.OutputColumns[Index];
  }

 /**
  * Get the output column value as a double
  *
  * @param Index The column index to retrieve the data for
  * @return The column value as a double
  */
  public double getOutputColumnValueDouble(int Index)
  {
    return (Double)this.OutputColumns[Index];
  }

 /**
  * Get the output column value as a long
  *
  * @param Index The column index to retrieve the data for
  * @return The column value as a long
  */
  public long getOutputColumnValueLong(int Index)
  {
    return (Long)this.OutputColumns[Index];
  }

 /**
  * Get the output column value as a long date
  *
  * @param Index The column index to retrieve the data for
  * @return The column value as a long date
  */
  public Long getOutputColumnValueDateAsLong(int Index)
  {
    return (Long)this.OutputColumns[Index];
  }

 /**
  * Get the type of the output column
  *
  * @param Index the column to get
  * @return The type
  */
  public int getOutputColumnType(int Index)
  {
    return this.OutputColumnTypes[Index];
  }

 /**
  * Return the columns as a single separated string
  *
  * @return The columns as a CSV string
  */
  public String getDataString()
  {
    int i;
    StringBuilder tmpReassemble;

    // We use the string buffer for the reassembly of the record. Avoid
    // just catenating strings, as it is a LOT slower because of the
    // java internal string handling (it has to allocate/deallocate many
    // times to rebuild the string).
    tmpReassemble = new StringBuilder(1024);

    for (i=0;i< this.getOutputColumnCount(); i++)
    {
      if (i == 0)
      {
        tmpReassemble.append(this.getOutputColumns()[i]);
      }
      else
      {
        tmpReassemble.append(";");
        tmpReassemble.append(this.getOutputColumns()[i]);
      }
    }

    return tmpReassemble.toString();
  }
}
