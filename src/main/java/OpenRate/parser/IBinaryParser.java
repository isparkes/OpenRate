

package OpenRate.parser;

/**
 * ASN.1 file parser
 *
 * @author Magnus
 */
public interface IBinaryParser
{
  /**
   * Set the data to be parsed
   *
   * @param data The data we want to parse
   */
  public void setDataToParse(byte[] data);
}

