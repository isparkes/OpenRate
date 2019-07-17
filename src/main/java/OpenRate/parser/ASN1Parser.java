package OpenRate.parser;

import OpenRate.exception.ASN1Exception;
import java.util.Formatter;

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
  public static final int CONSTRUCTED     = 0x00;

 /**
  * 170: BCD String (Octet String)
  */
  public static final int BCDString       = 0xAA;

 /**
  * 171: BCD String (Octet String), little endian format
  */
  public static final int BCDStringLE     = 0xAB;

  /* Types - not used in v.1, maybe when improved further */
 /**
  * 1: Boolean
  */
  public static final int BOOLEAN         = 0x01;

 /**
  * 2: Integer
  */
  public static final int INTEGER         = 0x02;

 /**
  * 2: Bit string
  */
  public static final int BITSTRING       = 0x03;

 /**
  * 4: Byte string
  */
  public static final int OCTETSTRING     = 0x04;

 /**
  * 5: NULL
  */
  public static final int NULLTAG         = 0x05;

 /**
  * 6: Object Identifier
  */
  public static final int OID             = 0x06;

 /**
  * 7: Object Descriptor
  */
  public static final int OBJDESCRIPTOR   = 0x07;

 /**
  * 8: External
  */
  public static final int EXTERNAL        = 0x08;

 /**
  * 9: Real
  */
  public static final int REAL            = 0x09;

 /**
  * 10: Enumerated
  */
  public static final int ENUMERATED      = 0x0A;

 /**
  * 11: Embedded Presentation Data Value
  */
  public static final int EMBEDDED_PDV    = 0x0B;

 /**
  * 12: UTF8 string
  */
  public static final int UTF8STRING      = 0x0C;

 /**
  * 16: Sequence/sequence of
  */
  public static final int SEQUENCE        = 0x10;

 /**
  * 17: Set/set of
  */
  public static final int SET             = 0x11;

 /**
  * 18: Numeric string
  */
  public static final int NUMERICSTRING   = 0x12;

 /**
  * 19: Printable string (ASCII subset)
  */
  public static final int PRINTABLESTRING = 0x13;

 /**
  * 20: T61/Teletex string
  */
  public static final int T61STRING       = 0x14;

 /**
  * 21: Videotex string
  */
  public static final int VIDEOTEXSTRING  = 0x15;

 /**
  * 22: IA5/ASCII string
  */
  public static final int IA5STRING       = 0x16;

 /**
  * 23: UTC time
  */
  public static final int UTCTIME         = 0x17;

 /**
  * 24: Generalized time
  */
  public static final int GENERALIZEDTIME = 0x18;

 /**
  * 25: Graphic string
  */
  public static final int GRAPHICSTRING   = 0x19;

 /**
  * 26: Visible string (ASCII subset)
  */
  public static final int VISIBLESTRING   = 0x1A;

 /**
  * 27: General string
  */
  public static final int GENERALSTRING   = 0x1B;

 /**
  * 28: Universal string
  */
  public static final int UNIVERSALSTRING = 0x1C;

 /**
  * 30: Basic Multilingual Plane/Unicode string
  */
  public static final int BMPSTRING       = 0x1E;

  // The definition file we are using
  private IASN1Def ASN1Def;

  // The byte stream reader for this decoder
  private ASN1Parser.ByteArrayReader reader = new ASN1Parser.ByteArrayReader();

 /**
  * Find out whether the reader is ready for another call
  *
  * @return true if ready, otherwise false
  */
  public boolean ready()
  {
    return reader.ready();
  }

 /**
  * The internal reader object
  */
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
    * Set the internal data buffer and reset the pointer.
    * 
    * @param data The data to set
    */
    private void setData(byte[]data) {
      this.byteArrayLength = data.length;
      this.bytePosition = 0;
      this.data = data;
    }

   /**
    * Read a single byte out of the reader, moving the byte pointer on to
    * be ready for the next byte.
    * 
    * @return 
    */
    public byte readByte() {
      byte output;
      output=this.data[bytePosition];
      this.bytePosition += 1;
      return output;
    }

   /**
    * Returns true if there are more bytes to read, otherwise false.
    * 
    * @return True if there are more bytes left in the reader
    */
    public boolean ready() {
      return (this.byteArrayLength > this.bytePosition);
    }

   /**
    * Chop out an array section from an existing array.
    * 
    * @param header The original byte array
    * @param length The number of bytes to chop
    * @return The new chopped byte array
    */
    public byte[] chopByteArray(byte[] header, int length) {
      byte[] newhead = new byte[length];
      System.arraycopy(header, 0, newhead, 0, length);
      return newhead;
    }
  }

  /**
   * Set the data to be parsed.
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
   * formats the value of the tag as an integer and return as a BCD string value.
   * No value checking is done. Padding is removed.
   * 
   * @param value the input BCD encoded array
	 * @return The decoded string
	 *
	 */
	public String parseBCDString(byte[] value) {
		StringBuilder buf = new StringBuilder(value.length * 2);

		for (int i = 0; i < value.length; ++i) {
      int hiNibble = ((value[i] & 0xf0) >> 4);
      int loNibble = (value[i] & 0x0f);
			if ((i != value.length) && (hiNibble != 0x0f)) // if not pad char
  			buf.append((char) (hiNibble + '0'));
			if ((i != value.length) && (loNibble != 0x0f)) // if not pad char
				buf.append((char) (loNibble + '0'));
		}
		return buf.toString();
    }

  /**
   * formats the value of the tag as an integer and return as a BCD string value.
   * No value checking is done. Padding is removed. The nibbles of the BCD are
   * reversed, à la Ericsson.
   * 
   * @param value the input BCD encoded array
	 * @return The decoded string
	 *
	 */
	public String parseBCDStringLE(byte[] value) {
		StringBuilder buf = new StringBuilder(value.length * 2);

		for (int i = 0; i < value.length; ++i) {
      int loNibble = ((value[i] & 0xf0) >> 4);
      int hiNibble = (value[i] & 0x0f);
			if ((i != value.length) && (hiNibble != 0x0f)) // if not pad char
  			buf.append((char) (hiNibble + '0'));
			if ((i != value.length) && (loNibble != 0x0f)) // if not pad char
				buf.append((char) (loNibble + '0'));
		}
		return buf.toString();
    }

  /**
   * formats the value of the tag as an integer and return as a string value.
   * No value checking is done.
   *
   * @param value the input hexadecimal byte encoded array
   * @return The parsed value
   */
  public String parseInteger(byte[] value) {
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
   * formats the value of the tag as an integer and return as an integer value.
   * No value checking is done.
   *
   * @param value the input hexadecimal byte encoded array
   * @return The parsed value
   */
  public int parseIntegerAsInteger(byte[] value) {
    boolean negative;
    int sum_up = 0;

    if ( value != null ) {
      negative = ((value[0]>>7) != 0);
      sum_up=0;

      for(int i = 0; i < value.length; i++)
      {
        sum_up<<=8;
        sum_up+=(long)(value[i] & 0xFF);
        if (negative) sum_up-=0x01<<(8*value.length);
      }
    }
    return sum_up;
  }
  
  /**
   * formats the value of the tag as a printable string. No value checking
   * is done.
   *
   * @param value the input hexadecimal byte encoded array
   * @return The string
   */
  public String parsePrintableString(byte[] value)
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
   * formats the value of the tag as an IA5String. No value checking
   * is done.
   * 
   * @param value the input hexadecimal byte encoded array
	 * @return The decoded string
   */
    public String parseIA5String(byte[] value) {
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
   * formats the value of the tag as a hexadecimal byte array. Value checking
   * is done. If a null value is passed in, we pass back an empty string.
   * 
   * @param value the input hexadecimal byte encoded array
	 * @return The decoded string
	 */
	public String parseBytes(byte[] value) {
    if (value == null)
    {
      return "";
    }
    else
    {
      StringBuilder buf = new StringBuilder(value.length * 2);

      Formatter formatter = new Formatter(buf);  
      for (byte b : value) {  
          formatter.format("%02x", b);  
      }
      return buf.toString();
    }
  }

  /**
   * formats the value of the tag as a hexadecimal byte array. Value checking
   * is done. If a null value is passed in, we pass back an empty string.
   * 
   * @param value the input hexadecimal byte encoded array
	 * @return The decoded string
	 */
	public String parseBytes(byte[] value, int length) {
    if (value == null)
    {
      return "";
    }
    else
    {
      StringBuilder buf = new StringBuilder(value.length * 2);

      Formatter formatter = new Formatter(buf);  
      for (byte b : value) {  
          formatter.format("%02x", b);  
      }
      return buf.toString().substring(0, length*2);
    }
  }

  /**
   * Parse the ASN.1
   *
   * @param tag The tag
   * @param value The value byte array
   * @return The string
   * @throws ASN1Exception
   */
  public String parseASN1(int tagType, byte[] value) throws ASN1Exception
    {
        String output ="";
        if ( value != null ) {
            switch (tagType) {
                case INTEGER:         output=parseInteger(value); break;
                case PRINTABLESTRING: output=parsePrintableString(value); break;
                case OCTETSTRING:     output=parsePrintableString(value); break;
                case IA5STRING:       output=parseIA5String(value); break;
                case BCDString:       output=parseBCDString(value); break;
                case BCDStringLE:     output=parseBCDStringLE(value); break;
                default:              output=parseBytes(value);break;
            }
        }
        return output;
    }

  /**
   * Reads a block out of the byte stream without looking at the contents. This
   * allows us to easily separate records out of logical streams and treat them
   * recursively or procedurally.
   *
   * @param length The length of the block to return
   * @return The block
   */
  public byte[] readBlock(int length)
  {
    byte[] block = new byte[length];
    
    for (int idx = 0 ; idx < length ; idx++)
      block[idx] = reader.readByte();
    
    return block;
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
    int length;

    /* Local variables */
    int index = 0;
    byte value;
    byte[] header = new byte[5];

    // Get the first byte for analysis
    byte nextByte = reader.readByte();
        
    // if this is a filler byte skip it - this is used as packing in some
    // formats e.g. Ericsson to get to the end of a block boundary
    if (nextByte == 0x00)
    {
      output.setNullTag(true);
      return output;
    }
    
    output.setTag(nextByte);

    header[0] = (byte) output.getTag();
    output.setId(output.getTag() & ~output.TAG_MASK);

    if ((output.getTag() & output.TAG_MASK) == output.TAG_MASK) {
      /* Long tag encoded as sequence of 7-bit values.  This doesn't try to
      handle tags > INT_MAX, it'd be pretty peculiar ASN.1 if it had to
      use tags this large */
      output.setTag(0);
      
      do {
        value = reader.readByte();
        header[index + 1] = value;
        output.setTag((output.getTag() << 7) | (value & 0x7F));
        index++;
      } while (((value & output.LEN_XTND) != 0) && (index < 5) && (reader.ready()));

      // If we ran off the end of the header, it is an error
      if (index == 5) {
        return null;
      }
      
      // set the tag
      output.setTagFromByteArray(header);
      
      // set the raw tag
      output.setRawTag(parseBytes(header,index+1));
    }
    else
    {
      // Simple 1 byte tag
      output.setTag(output.getTag() & output.TAG_MASK);
      output.setRawTag(parseBytes(header,1));
    }

    // Parse the length out of the stream
    length = reader.readByte();
    if (length == 0) {
      // it really is 0
      length = 0;
    } else if ((length & output.LEN_MASK) != length) {
      // This is a multibyte length.  Find the actual length
      int numLengthBytes = (length & output.LEN_MASK);
      length = 0x00000000;
      byte[] buffer = new byte[numLengthBytes];
      try {
        buffer = readBlock(numLengthBytes);
      } catch (Exception ex) {
        throw ex;
      }

      switch (numLengthBytes) {
        case 0:
          // we have a zero length
          break;
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
          throw new Exception("Length cannot be represented as a Java int");
      }
    }
    output.setLength(length);
    if (!output.isConstructed()) {
      output.setValue(readBlock(output.getLength()));
    }
    
    return output;
  }
}

