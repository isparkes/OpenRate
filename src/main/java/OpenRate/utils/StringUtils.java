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

package OpenRate.utils;

/**
 * StringUtils - this optimization performs better than the JDK version under
 * 1.3.1, but roughly equivalently (or sometimes slower) under 1.4.2. Therefore
 * only use it if you <b>have to</b> and you are using an older JDK. It's use
 * is no longer recommended.
 *
 */
public class StringUtils
{
  /**
   * Constructor for StringUtils.
   */
  public StringUtils()
  {
    super();
  }

  /**
   * This method runs about 20 times faster than java.lang.String.toLowerCase
   * (and doesn't waste any storage when the result is equal to the input).
   * Warning: Don't use this method when your default locale is Turkey.
   *
   * java.lang.String.toLowerCase is slow because (a) it uses a StringBuffer
   * (which has synchronized methods), (b) it initializes the StringBuffer to
   * the default size, and (c) it gets the default locale every time to test
   * for name equal to "tr".
   *
   * @param str The string to convert
   * @return The converted string
   * @author Peter Norvig
   */
  public static String toLowerCase(String str)
  {
    int len       = str.length();
    int different = -1;

    // See if there is a char that is different in lowercase
    for (int i = len - 1; i >= 0; i--)
    {
      char ch = str.charAt(i);

      if (Character.toLowerCase(ch) != ch)
      {
        different = i;

        break;
      }
    }

    // If the string has no different char, then return the string as is,
    // otherwise create a lowercase version in a char array.
    if (different == -1)
    {
      return str;
    }
    else
    {
      char[] chars = new char[len];
      str.getChars(0, len, chars, 0);

      // (Note we start at different, not at len.)
      for (int j = different; j >= 0; j--)
      {
        chars[j] = Character.toLowerCase(chars[j]);
      }

      return new String(chars);
    }
  }

  /**
   * This method runs about 20 times faster than java.lang.String.toUpperCase
   * (and doesn't waste any storage when the result is equal to the input).
   * Warning: Don't use this method when your default locale is Turkey.
   *
   * java.lang.String.toUpperCase is slow because (a) it uses a StringBuffer
   * (which has synchronized methods), (b) it initializes the StringBuffer
   * to the default size, and (c) it gets the default locale every time to
   * test for name equal to "tr".
   *
   * @param str The string to convert
   * @return The converted string
   * @author Peter Norvig
   */
  public static String toUpperCase(String str)
  {
    int len       = str.length();
    int different = -1;

    // See if there is a char that is different in uppercase
    for (int i = len - 1; i >= 0; i--)
    {
      char ch = str.charAt(i);

      if (Character.toUpperCase(ch) != ch)
      {
        different = i;

        break;
      }
    }

    // If the string has no different char, then return the string as is,
    // otherwise create a lowercase version in a char array.
    if (different == -1)
    {
      return str;
    }
    else
    {
      char[] chars = new char[len];
      str.getChars(0, len, chars, 0);

      // (Note we start at different, not at len.)
      for (int j = different; j >= 0; j--)
      {
        chars[j] = Character.toUpperCase(chars[j]);
      }

      return new String(chars);
    }
  }
}
