/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
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

package OpenRate.parser;

import OpenRate.exception.ASN1Exception;

/**
 * ASN.1 file parser
 *
 * @author Magnus
 */
public class ASN1Parser implements IBinaryParser
{
  /* Own types - used to make sure the right parsing is used */

 /**
  * 0: Constructed (Sequence/Choice...)
  */
  public final int CONSTRUCTED     = 0x00;

 /**
  * 170: BCD String (Octet String)
  */
  public final int BCDString       = 0xAA;

  /* Types - not used in v.1, maybe when improved further */
 /**
  * 1: Boolean
  */
  public final int BOOLEAN         = 0x01;

 /**
  * 2: Integer
  */
  public final int INTEGER         = 0x02;

 /**
  * 2: Bit string
  */
  public final int BITSTRING       = 0x03;

 /**
  * 4: Byte string
  */
  public final int OCTETSTRING     = 0x04;

 /**
  * 5: NULL
  */
  public final int NULLTAG         = 0x05;

 /**
  * 6: Object Identifier
  */
  public final int OID             = 0x06;

 /**
  * 7: Object Descriptor
  */
  public final int OBJDESCRIPTOR   = 0x07;

 /**
  * 8: External
  */
  public final int EXTERNAL        = 0x08;

 /**
  * 9: Real
  */
  public final int REAL            = 0x09;

 /**
  * 10: Enumerated
  */
  public final int ENUMERATED      = 0x0A;

 /**
  * 11: Embedded Presentation Data Value
  */
  public final int EMBEDDED_PDV    = 0x0B;

 /**
  * 12: UTF8 string
  */
  public final int UTF8STRING      = 0x0C;

 /**
  * 16: Sequence/sequence of
  */
  public final int SEQUENCE        = 0x10;

 /**
  * 17: Set/set of
  */
  public final int SET             = 0x11;

 /**
  * 18: Numeric string
  */
  public final int NUMERICSTRING   = 0x12;

 /**
  * 19: Printable string (ASCII subset)
  */
  public final int PRINTABLESTRING = 0x13;

 /**
  * 20: T61/Teletex string
  */
  public final int T61STRING       = 0x14;

 /**
  * 21: Videotex string
  */
  public final int VIDEOTEXSTRING  = 0x15;

 /**
  * 22: IA5/ASCII string
  */
  public static final int IA5STRING       = 0x16;

 /**
  * 23: UTC time
  */
  public final int UTCTIME         = 0x17;

 /**
  * 24: Generalized time
  */
  public final int GENERALIZEDTIME = 0x18;

 /**
  * 25: Graphic string
  */
  public final int GRAPHICSTRING   = 0x19;

 /**
  * 26: Visible string (ASCII subset)
  */
  public final int VISIBLESTRING   = 0x1A;

 /**
  * 27: General string
  */
  public final int GENERALSTRING   = 0x1B;

 /**
  * 28: Universal string
  */
  public final int UNIVERSALSTRING = 0x1C;

 /**
  * 30: Basic Multilingual Plane/Unicode string
  */
  public final int BMPSTRING       = 0x1E;

  private IASN1Def ASN1Def;

  private ByteArrayReader reader = new ByteArrayReader();

 /**
  * Find out whether the reader is ready for another call
  *
  * @return true if ready, otherwise false
  */
  public boolean ready()
  {
    return reader.ready();
  }

  private class ByteArrayReader {
    private byte[] data;
    private int byteArrayLength = 0;
    private int bytePosition = 0;

   /**
    * Constructor to set the byte array reader to the given value.
    *
    * @param data The binary data to set.
    */
    public ByteArrayReader(byte[] data)
    {
      // Set the internal data buffer with the given data
      setData(data);
    }

   /**
    * Default constructor.
    */
    public ByteArrayReader()
    {
      // Nothing
    }

   /**
    * Set the internal data buffer and reset the pointer
    * @param data
    */
    private void setData(byte[]data) {
      this.byteArrayLength = data.length;
      this.bytePosition = 0;
      this.data = data;
    }

    public byte readByte() {
      byte output;
      output=this.data[bytePosition];
      this.bytePosition += 1;
      return output;
    }

    public byte[] readBytes(byte[] buffer) throws Exception {
      return chopByteArray(buffer.length);
    }

    public boolean ready() {
      return (this.byteArrayLength > this.bytePosition);
    }

    public byte[] chopByteArray(int length) throws Exception {
      byte[] newhead = new byte[length];

      if (this.byteArrayLength >= (this.bytePosition + length)) {
        length += this.bytePosition;
        int headCount = 0;
        for (int i = this.bytePosition; i < length; i++) {
          newhead[headCount] = this.data[i];
          headCount += 1;
        }
        this.bytePosition = length;
      } else {
        throw new Exception("Position + choplength (" + this.bytePosition + "+" + length +") puts choplength beyond length of array ( "+ this.byteArrayLength + ").");
      }
      return newhead;
    }

    public byte[] chopByteArray(byte[] header, int length) {
      byte[] newhead = new byte[length];
      System.arraycopy(header, 0, newhead, 0, length);
      return newhead;
    }
  }

  /**
   * Set the data to be parsed
   *
   * @param data The data to be parsed
   */
  @Override
  public void setDataToParse(byte[] data)
    {
      reader.setData(data);
    }

    /**
   * Create a new ASN.1 parser using the supplied specification
   *
   * @param ASN1Specification The specification to use
   */
  public ASN1Parser(IASN1Def ASN1Specification)
  {
    ASN1Def = ASN1Specification;
  }

  /**
   * Parse an ASN.1 name
   *
   * @param tag The tag number to get the name for
   * @return The name of the tag
   */
  public String parseASN1Name(int tag)
  {
      try {
          return ASN1Def.getTagName(tag);
      } catch (Exception e) {
          return "";
      }
  }

  /**
   * Get the type for a tag
   *
   * @param tag The tag to get
   * @return The type
   */
  public int getType(int tag) {
      return this.ASN1Def.getType(tag);
  }

  /**
   * Get the type name for a tag
   *
   * @param tag The tag
   * @return The type name
   */
  public String getTypeName(int tag) {
      return this.ASN1Def.getTypeName(tag);
  }

  /**
   * @param tag
	 *            the tag number
   * @param value
	 *            the input BCD encoded array
	 * @return The decoded string
	 *
	 */
	public String parseBCDString(int tag, byte[] value) {
		StringBuilder buf = new StringBuilder(value.length * 2);

		for (int i = 0; i < value.length; ++i) {
			buf.append((char) (((value[i] & 0xf0) >> 4) + '0'));
			if ((i != value.length) && ((value[i] & 0xf) != 0x0A)) // if not pad char
				buf.append((char) ((value[i] & 0x0f) + '0'));
		}
		return buf.toString();
    }

  /**
   * Parse an integer out
   *
   * @param tag The tag to get the value for
   * @param value The value to parse
   * @return The parsed value
   * @throws ASN1Exception
   */
  public String parseInteger(int tag, byte[] value) throws ASN1Exception {
        String output = "";
        boolean negative;
        long sum_up;

        if ( value != null ) {
            negative = ((value[0]>>7) != 0);
            sum_up=0;

            for(int i = 0; i < value.length; i++)
            {
                sum_up<<=8;
                sum_up+=(long)(value[i] & 0xFF);
                if (negative) sum_up-=0x01<<(8*value.length);
            }

            output += "" + sum_up;
        }
        return output;
    }

  /**
   * Parse a string
   *
   * @param tag The tag
   * @param value The byte array of values
   * @return The string
   */
  public String parsePrintableString(int tag, byte[] value)
    {
        String output = "";

        if (value != null )
        {
            for(int i = 0; i < value.length; i++) {
                output += (char)value[i];
            }
        }

        return output;
    }

  /**
   * Parse the value as an IA5String
   *
   * @param tag The tag
   * @param value The byte array of values
   * @return The string
   */
    private String parseIA5String(int tag, byte[] value) {
      String output = "";
      int i,len = value.length;
      for (i = 0; i < len; ++i) {
          char c = (char) ( value[i] & 0xFF );
          if ((c < 0) || (c > 127)) return "";
          output += (char)( value[i] & 0xFF );
      }
      return output;
    }

  /**
   * Parse the ASN.1
   *
   * @param tag The tag
   * @param value The value byte array
   * @return The string
   * @throws ASN1Exception
   */
  public String parseASN1(int tag, byte[] value) throws ASN1Exception
    {
        String output ="";
        if ( value != null ) {
            switch (this.getType(tag)) {
                case INTEGER:         output=parseInteger(tag, value); break;
                case PRINTABLESTRING: output=parsePrintableString(tag, value); break;
                case OCTETSTRING:     output=parsePrintableString(tag, value); break;
                case IA5STRING:       output=parseIA5String(tag, value); break;
                case 0xAA:  output=parseBCDString(tag, value); break;
                default: output="";break;
            }
        }
        return output;
    }

  private byte[] readValue(int length) throws Exception {
    byte[] value = new byte[length];
    value = reader.readBytes(value);
    return value;
  }

  /**
   * Reads the next element in the parsing sequence
   *
   * @return The next element
   * @throws Exception
   */
  public Asn1Class readNextElement() throws Exception
  {
    Asn1Class output = new Asn1Class();
    int tag;
    int length;

    /* Local variables */
    int index = 0;
    int value;
    byte[] header = new byte[5];

    output.setTag(reader.readByte());

    header[0] = (byte) output.getTag();
    output.setId(output.getTag() & ~output.TAG_MASK);
    output.setTag(output.getTag() & output.TAG_MASK);

    if (output.getTag() == output.TAG_MASK) {
      /* Long tag encoded as sequence of 7-bit values.  This doesn't try to
      handle tags > INT_MAX, it'd be pretty peculiar ASN.1 if it had to
      use tags this large */
      output.setTag(0);
      do {
        value = reader.readByte();
        header[index + 1] = (byte) value;
        output.setTag((output.getTag() << 7) | (value & 0x7F));
        index++;
      } while (((value & output.LEN_XTND) != 0) && (index < 5) && (reader.ready()));
      if (index == 5) {
        return null;
      }
    }

    output.setTagFromByteArray(reader.chopByteArray(header, index + 1));
    output.setTagname(ASN1Def.getTagName(output.getTag()));

    length = reader.readByte();
    if ((length & output.LEN_MASK) == 0x00) {
      length = 128;
    } else if ((length & output.LEN_MASK) != length) {
      // This is a multibyte length.  Find the actual length
      int numLengthBytes = (length & output.LEN_MASK);
      length = 0x00000000;
      byte[] buffer = new byte[numLengthBytes];
      try {
        buffer = reader.readBytes(buffer);
      } catch (Exception ex) {
        throw ex;
      }

      switch (numLengthBytes) {
        case 1:
          length |= (0x000000FF & buffer[0]);
          break;
        case 2:
          length |= ((0x000000FF & buffer[0]) << 8)
                  | (0x000000FF & buffer[1]);
          break;
        case 3:
          length |= ((0x000000FF & buffer[0]) << 16)
                  | ((0x000000FF & buffer[1]) << 8)
                  | (0x000000FF & buffer[2]);
          break;
        case 4:
          length |= ((0x000000FF & buffer[0]) << 24)
                  | ((0x000000FF & buffer[1]) << 16)
                  | ((0x000000FF & buffer[2]) << 8)
                  | (0x000000FF & buffer[3]);
          break;
        default:
          throw new Exception("Length cannot be represented as "
                  + "a Java int");
      }
    }
    output.setLength(length);
    if (!output.getConstructed()) {
      output.setValue(readValue(output.getLength()));
    }
    return output;
  }

  /**
   * Reads the next tag
   *
   * @param tag
   * @throws Exception
   */
  public void readNextTag(int tag) throws Exception {
    Asn1Class item;

    item = readNextElement();
    long endOfTag = item.getLength() + reader.bytePosition;
  }
}

