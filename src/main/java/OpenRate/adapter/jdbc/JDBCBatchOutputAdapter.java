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
package OpenRate.adapter.jdbc;

import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.DBRecord;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

/**
 * Please <a target='new'
 * href='http://www.open-rate.com/wiki/index.php?title=JDBC_Batch
 * Output_Adapter'>click here</a> to go to wiki page.
 *
 * <p>
 * JDBC Batch Output Adapter.<br>
 *
 * This is a higher performance version of the JDBC output adapter, which
 * performs batch commits. The rest of the operation is the same as the parent
 * version JDBC output adapter, "JDBCOutputAdapter".
 */
public abstract class JDBCBatchOutputAdapter
        extends JDBCOutputAdapter {

  /**
   * Default constructor
   */
  public JDBCBatchOutputAdapter() {
    super();
  }

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited Plug In functions ---------------------
  // -----------------------------------------------------------------------------
  /**
   * Initialise the module. Called during pipeline creation. Initialise the
   * Logger, and load the SQL statements.
   *
   * @param PipelineName The name of the pipeline this module is in
   * @param ModuleName The module symbolic name of this module
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void init(String PipelineName, String ModuleName)
          throws InitializationException {
    // perform the initialisation
    super.init(PipelineName, ModuleName);

    try {
      // see if we can do batch commits
      JDBCcon = DBUtil.getConnection(dataSourceName);
      if (JDBCcon.getMetaData().supportsBatchUpdates() == false) {
        message = "Output <" + getSymbolicName() + "> does not support batch commits in adapter <" + getSymbolicName() + ">. Please use non-Batch adapter.";
        getPipeLog().fatal(message);
        throw new InitializationException(message, getSymbolicName());
      }

      // Done the check, close it
      JDBCcon.close();
    } catch (SQLException Sex) {
      message = "Output <" + getSymbolicName() + "> error setting manual commit in adapter <" + getSymbolicName() + ">. message <" + Sex.getMessage() + ">";
      getPipeLog().fatal(message);
      throw new InitializationException(message, getSymbolicName());
    }
  }

  /**
   * Process the stream header. Get the file base name and open the transaction.
   *
   * @param r The record we are working on
   * @return The processed record
   * @throws ProcessingException
   */
  @Override
  public HeaderRecord procHeader(HeaderRecord r) throws ProcessingException {
    // perform any parent processing first
    super.procHeader(r);

    try {
      // set the connection to use controlled commits
      JDBCcon.setAutoCommit(false);
    } catch (SQLException Sex) {
      // Not good. Abort the transaction
      message = "Error changing autocommit. message <" + Sex.getMessage() + "> in adapter <" + getSymbolicName() + ">. Aborting transaction.";
      getPipeLog().fatal(message);
      getExceptionHandler().reportException(new ProcessingException(Sex, getSymbolicName()));
      this.setTransactionAbort(getTransactionNumber());
    }

    return r;
  }

  /**
   * Prepare good records for writing to the defined output stream.
   *
   * @param r The current record we are working on
   * @return The prepared record
   * @throws ProcessingException
   */
  @Override
  public IRecord prepValidRecord(IRecord r) throws ProcessingException {
    int i;
    Collection<DBRecord> outRecCol = null;
    DBRecord outRec;
    Iterator<DBRecord> outRecIter;

    try {
      outRecCol = procValidRecord(r);
    } catch (ProcessingException pe) {
      // Pass the exception up
      message = "Processing exception preparing valid record in module <"
              + getSymbolicName() + ">. message <" + pe.getMessage()
              + ">. Aborting transaction.";
      getPipeLog().fatal(message);
      getExceptionHandler().reportException(new ProcessingException(pe, getSymbolicName()));
      setTransactionAbort(getTransactionNumber());
    } catch (ArrayIndexOutOfBoundsException aiex) {
      // Not good. Abort the transaction
      message = "Column Index preparing valid record in module <"
              + getSymbolicName() + ">. message <" + aiex.getMessage()
              + ">. Aborting transaction.";
      getPipeLog().fatal(message);
      getExceptionHandler().reportException(new ProcessingException(message, aiex, getSymbolicName()));
      setTransactionAbort(getTransactionNumber());
    } catch (Exception ex) {
      // Not good. Abort the transaction
      message = "Unexpected Exception preparing valid record in module <"
              + getSymbolicName() + ">. message <" + ex.getMessage()
              + ">. Aborting transaction.";
      getPipeLog().fatal(message);
      getExceptionHandler().reportException(new ProcessingException(message, ex, getSymbolicName()));
      setTransactionAbort(getTransactionNumber());
    }

    // Null return means "do not bother to process"
    if (outRecCol != null) {
      outRecIter = outRecCol.iterator();

      while (outRecIter.hasNext()) {
        outRec = outRecIter.next();

        try {
          // Prepare the parameter values
          stmtInsertQuery.clearParameters();

          for (i = 0; i < outRec.getOutputColumnCount(); i++) {
            if (outRec.getOutputColumnType(i) == DBRecord.COL_TYPE_STRING) {
              // String value
              stmtInsertQuery.setString(i + 1, outRec.getOutputColumnValueString(i));
            } else if (outRec.getOutputColumnType(i) == DBRecord.COL_TYPE_INTEGER) {
              // Integer value
              stmtInsertQuery.setInt(i + 1, outRec.getOutputColumnValueInt(i));
            } else if (outRec.getOutputColumnType(i) == DBRecord.COL_TYPE_DOUBLE) {
              // Double value
              stmtInsertQuery.setDouble(i + 1, outRec.getOutputColumnValueDouble(i));
            } else if (outRec.getOutputColumnType(i) == DBRecord.COL_TYPE_LONG) {
              // Long value
              stmtInsertQuery.setLong(i + 1, outRec.getOutputColumnValueLong(i));
            } else if (outRec.getOutputColumnType(i) == DBRecord.COL_TYPE_DATE) {
              // Date value
              long DateValue = outRec.getOutputColumnValueLong(i);
              java.sql.Date DateToSet;
              DateToSet = new java.sql.Date(DateValue);
              stmtInsertQuery.setDate(i + 1, DateToSet);
            } else if (outRec.getOutputColumnType(i) == DBRecord.COL_TYPE_BOOL) {
              // Boolean value
              boolean value;
              value = outRec.getOutputColumnValueString(i).equals("1");
              stmtInsertQuery.setBoolean(i + 1, value);
            } else if (outRec.getOutputColumnType(i) == DBRecord.COL_TYPE_TIME) {
              // Time value
              long DateValue = outRec.getOutputColumnValueLong(i);
              java.sql.Time TimeToSet;
              TimeToSet = new java.sql.Time(DateValue);
              stmtInsertQuery.setTime(i + 1, TimeToSet);
            } else if (outRec.getOutputColumnType(i) == DBRecord.COL_TYPE_BINARY) {
              // Binary value
              byte[] byteArray = outRec.getOutputColumnValueBytes(i);
              stmtInsertQuery.setBytes(i + 1, byteArray);
            }
          }

          stmtInsertQuery.addBatch();
        } catch (SQLException Sex) {
          // Not good. Abort the transaction
          message = "SQL Exception inserting valid record in module <"
                  + getSymbolicName() + ">. message <" + Sex.getMessage()
                  + ">. Aborting transaction.";
          getPipeLog().fatal(message);
          getExceptionHandler().reportException(new ProcessingException(message, Sex, getSymbolicName()));
          setTransactionAbort(getTransactionNumber());
        } catch (ArrayIndexOutOfBoundsException aiex) {
          // Not good. Abort the transaction
          message = "Column Index inserting valid record in module <"
                  + getSymbolicName() + ">. message <" + aiex.getMessage()
                  + ">. Aborting transaction.";
          getPipeLog().fatal(message);
          getExceptionHandler().reportException(new ProcessingException(message, aiex, getSymbolicName()));
          setTransactionAbort(getTransactionNumber());
        } catch (NumberFormatException nfe) {
          // Not good. Abort the transaction
          // Not good. Abort the transaction
          message = "Number format inserting valid record in module <"
                  + getSymbolicName() + ">. message <" + nfe.getMessage()
                  + ">. Aborting transaction.";
          getPipeLog().fatal(message);
          getExceptionHandler().reportException(new ProcessingException(message, nfe, getSymbolicName()));
          setTransactionAbort(getTransactionNumber());
        } catch (Exception ex) {
          // Not good. Abort the transaction
          message = "Unknown Exception inserting valid record in module <"
                  + getSymbolicName() + ">. message <" + ex.getMessage()
                  + ">. Aborting transaction.";
          getPipeLog().fatal(message);
          getExceptionHandler().reportException(new ProcessingException(message, ex, getSymbolicName()));
          setTransactionAbort(getTransactionNumber());
        }
      }
    }

    return r;
  }

  /**
   * Prepare bad records for writing to the defined output stream.
   *
   * @param r The current record we are working on
   * @return The prepared record
   * @throws ProcessingException
   */
  @Override
  public IRecord prepErrorRecord(IRecord r) throws ProcessingException {
    int i;
    Collection<DBRecord> outRecCol = null;
    DBRecord outRec;
    Iterator<DBRecord> outRecIter;

    try {
      outRecCol = procErrorRecord(r);
    } catch (ProcessingException pe) {
      // Pass the exception up
      message = "Processing exception preparing error record in module <"
              + getSymbolicName() + ">. message <" + pe.getMessage()
              + ">. Aborting transaction.";
      getPipeLog().fatal(message);
      getExceptionHandler().reportException(new ProcessingException(pe, getSymbolicName()));
      setTransactionAbort(getTransactionNumber());
    } catch (ArrayIndexOutOfBoundsException aiex) {
      // Not good. Abort the transaction
      message = "Column Index preparing error record in module <"
              + getSymbolicName() + ">. message <" + aiex.getMessage()
              + ">. Aborting transaction.";
      getPipeLog().fatal(message);
      getExceptionHandler().reportException(new ProcessingException(message, aiex, getSymbolicName()));
      setTransactionAbort(getTransactionNumber());
    } catch (Exception ex) {
      // Not good. Abort the transaction
      message = "Unknown Exception preparing error record in module <"
              + getSymbolicName() + ">. message <" + ex.getMessage()
              + ">. Aborting transaction.";
      getPipeLog().fatal(message);
      getExceptionHandler().reportException(new ProcessingException(message, ex, getSymbolicName()));
      setTransactionAbort(getTransactionNumber());
    }

    // Null return means "do not bother to process"
    if (outRecCol != null) {
      outRecIter = outRecCol.iterator();

      while (outRecIter.hasNext()) {
        outRec = outRecIter.next();

        try {
          // Prepare the parameter values
          stmtInsertQuery.clearParameters();

          for (i = 0; i < outRec.getOutputColumnCount(); i++) {
            if (outRec.getOutputColumnType(i) == DBRecord.COL_TYPE_STRING) {
              // String value
              stmtInsertQuery.setString(i + 1, outRec.getOutputColumnValueString(i));
            } else if (outRec.getOutputColumnType(i) == DBRecord.COL_TYPE_INTEGER) {
              // Integer value
              stmtInsertQuery.setInt(i + 1, outRec.getOutputColumnValueInt(i));
            } else if (outRec.getOutputColumnType(i) == DBRecord.COL_TYPE_DOUBLE) {
              // Double value
              stmtInsertQuery.setDouble(i + 1, outRec.getOutputColumnValueDouble(i));
            } else if (outRec.getOutputColumnType(i) == DBRecord.COL_TYPE_LONG) {
              // Long value
              stmtInsertQuery.setLong(i + 1, outRec.getOutputColumnValueLong(i));
            } else if (outRec.getOutputColumnType(i) == DBRecord.COL_TYPE_DATE) {
              // Date value
              long DateValue = outRec.getOutputColumnValueLong(i);
              java.sql.Date DateToSet;
              DateToSet = new java.sql.Date(DateValue);
              stmtInsertQuery.setDate(i + 1, DateToSet);
            } else if (outRec.getOutputColumnType(i) == DBRecord.COL_TYPE_BOOL) {
              // Boolean value
              boolean value;
              value = outRec.getOutputColumnValueString(i).equals("1");
              stmtInsertQuery.setBoolean(i + 1, value);
            } else if (outRec.getOutputColumnType(i) == DBRecord.COL_TYPE_TIME) {
              // Time value
              long DateValue = outRec.getOutputColumnValueLong(i);
              java.sql.Time TimeToSet;
              TimeToSet = new java.sql.Time(DateValue);
              stmtInsertQuery.setTime(i + 1, TimeToSet);
            } else if (outRec.getOutputColumnType(i) == DBRecord.COL_TYPE_BINARY) {
              // Binary value
              byte[] byteArray = outRec.getOutputColumnValueBytes(i);
              stmtInsertQuery.setBytes(i + 1, byteArray);
            }
          }

          stmtInsertQuery.addBatch();
        } catch (SQLException Sex) {
          // Not good. Abort the transaction
          message = "SQL Exception inserting error record in module <"
                  + getSymbolicName() + ">. message <" + Sex.getMessage()
                  + ">. Aborting transaction.";
          getPipeLog().fatal(message);
          getExceptionHandler().reportException(new ProcessingException(message, Sex, getSymbolicName()));
          setTransactionAbort(getTransactionNumber());
        } catch (ArrayIndexOutOfBoundsException aiex) {
          // Not good. Abort the transaction
          message = "Column Index inserting error record in module <"
                  + getSymbolicName() + ">. message <" + aiex.getMessage()
                  + ">. Aborting transaction.";
          getPipeLog().fatal(message);
          getExceptionHandler().reportException(new ProcessingException(message, aiex, getSymbolicName()));
          setTransactionAbort(getTransactionNumber());
        } catch (NumberFormatException nfe) {
          // Not good. Abort the transaction
          message = "Number format inserting error record in module <"
                  + getSymbolicName() + ">. message <" + nfe.getMessage()
                  + ">. Aborting transaction.";
          getPipeLog().fatal(message);
          getExceptionHandler().reportException(new ProcessingException(message, nfe, getSymbolicName()));
          setTransactionAbort(getTransactionNumber());
        } catch (Exception ex) {
          // Not good. Abort the transaction
          message = "Unknown Exception inserting error record in module <"
                  + getSymbolicName() + ">. message <" + ex.getMessage()
                  + ">. Aborting transaction.";
          getPipeLog().fatal(message);
          getExceptionHandler().reportException(new ProcessingException(message, ex, getSymbolicName()));
          setTransactionAbort(getTransactionNumber());
        }
      }
    }

    return r;
  }

  /**
   * This is called when a data record is encountered. You should do any normal
   * processing here. Note that the result is a collection for the case that we
   * have to re-expand after a record compression input adapter has done
   * compression on the input stream.
   *
   * @param r The record we are working on
   * @return The collection of processed records
   * @throws ProcessingException
   */
  public abstract Collection<DBRecord> procValidRecord(IRecord r) throws ProcessingException;

  /**
   * This is called when a data record with errors is encountered. You should do
   * any processing here that you have to do for error records, e.g. statistics,
   * special handling, even error correction!
   *
   * @param r The record we are working on
   * @return The collection of processed records
   * @throws ProcessingException
   */
  public abstract Collection<DBRecord> procErrorRecord(IRecord r) throws ProcessingException;

  /**
   * Do any required processing prior to completing the batch block. The
   * flushBlock() method is called for block processed and is intended for batch
   * commit control.
   *
   * @throws OpenRate.exception.ProcessingException
   */
  @Override
  public void flushBlock() throws ProcessingException {
    // We have to determine between the case of a normal block flush and the
    // block flush at the end of the transaction. We want to flush at the
    // block boundaries, but not at the end of the stream, because flushStream()
    // already did that. We make this decision based on the fact that there
    // is a transaction open or not
    if (getTransactionNumber() > 0) {
      // We are still in a transaction - do the flush
      try {
        // perform the batch commit once per block
        stmtInsertQuery.executeBatch();

        // perform a commit once per block
        getPipeLog().debug("Adapter <" + getSymbolicName() + "> performing commit.");
        JDBCcon.commit();
      } catch (SQLException Sex) {
        message = "Error performing batch commit in module <" + getSymbolicName()
                + ">. message <" + Sex.getMessage() + ">. Aborting transaction.";
        getPipeLog().fatal(message);
        String Nextmessage = "Next message <" + Sex.getNextException().getMessage() + ">";
        getPipeLog().fatal(Nextmessage);
        this.setTransactionAbort(getTransactionNumber());
        throw new ProcessingException(message, getSymbolicName());
      } catch (Exception ex) {
        // Not good. Abort the transaction
        message = "Error performing batch commit in module <" + getSymbolicName()
                + ">. message <" + ex.getMessage() + ">. Aborting transaction.";
        getPipeLog().fatal(message);
        getExceptionHandler().reportException(new ProcessingException(message, ex, getSymbolicName()));
        setTransactionAbort(getTransactionNumber());
      }
    }

    super.flushBlock();
  }

  /**
   * Do any required processing prior to completing the stream. The
   * flushStream() method is called for transaction stream. This differs from
   * the flushBlock(), which is called at the end of each block and the
   * cleanup() method, which is called only once upon application shutdown.
   *
   * @throws OpenRate.exception.ProcessingException
   */
  @Override
  public void flushStream() throws ProcessingException {
    // We are still in a transaction - do the flush
    try {
      // perform the batch commit once per block
      stmtInsertQuery.executeBatch();

      // perform a commit once per block
      getPipeLog().debug("Adapter <" + getSymbolicName() + "> performing commit.");
      JDBCcon.commit();
    } catch (SQLException Sex) {
      message = "Error performing batch commit in module <" + getSymbolicName()
              + ">. message <" + Sex.getMessage() + ">. Aborting transaction.";
      getPipeLog().fatal(message);
      String Nextmessage = "Next message <" + Sex.getNextException().getMessage() + ">";
      getPipeLog().fatal(Nextmessage);
      this.setTransactionAbort(getTransactionNumber());
      throw new ProcessingException(message, getSymbolicName());
    } catch (Exception ex) {
      // Not good. Abort the transaction
      message = "Error performing batch commit in module <" + getSymbolicName()
              + ">. message <" + ex.getMessage() + ">. Aborting transaction.";
      getPipeLog().fatal(message);
      getExceptionHandler().reportException(new ProcessingException(message, ex, getSymbolicName()));
      setTransactionAbort(getTransactionNumber());
    }

    super.flushStream();
  }
}
