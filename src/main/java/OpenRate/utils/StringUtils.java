

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
