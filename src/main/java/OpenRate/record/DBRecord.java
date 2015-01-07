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

    this.RecordNumber      = RecordNumber;
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

    for (i=0;i<ColumnCount;i++)
    {
      if (i == 0)
      {
        tmpReassemble.append(this.OriginalColumns[i]);
      }
      else
      {
        tmpReassemble.append(";");
        tmpReassemble.append(this.OriginalColumns[i]);
      }
    }

    return tmpReassemble.toString();
  }
}
