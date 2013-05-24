/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2013.
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
 * Tiger Shore Management or its officially assigned agents be liable to any
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


import java.io.Serializable;

/**
 * Interface definition for Error records
 */
public interface IError extends Serializable
{
 /**
  * Get the ErrorType object, which is used to classify errors into logical
  * groups
  *
  * @return The error type of this record
  */
  public ErrorType getType();

 /**
  * Get the severity of the message
  *
  * @return The severity of the message
  */
  public int getSeverity();

 /**
  * Set the severity of the message
  *
  * @param severity The severity of the message
  */
  public void setSeverity(int severity);

 /**
  * Get the text of the message. Usually an error code like "ERR_SOMETHING_WRONG".
  *
  * @return The error text of the message
  */
  public String getMessage();

 /**
  * Set the message. Usually an error code like "ERR_SOMETHING_WRONG".
  *
  * @param Message The text of the message
  */
  public void setMessage(String Message);

 /**
  * Set the creating module of the error
  *
  * @param ModuleName The name of the module setting the error
  */
  public void setModuleName(String ModuleName);

 /**
  * Get the module name of this error
  *
  * @return The module name that set this error
  */
  public String getModuleName();

 /**
  * Set the error number (error code, like -100) of the error
  *
  * @param ErrorNumber The new error number
  */
  public void setErrorNumber(int ErrorNumber);

 /**
  * Get the error number of this error
  *
  * @return The error number of this error
  */
  public int getErrorNumber();

 /**
  * Set the description string of the error, this is additional information
  * that is over an above the error code or error message
  *
  * @param ErrorDescription The description of the error
  */
  public void setErrorDescription(String ErrorDescription);

 /**
  * Get the error description of this error
  *
  * @return The description of this error
  */
  public String getErrorDescription();
}
