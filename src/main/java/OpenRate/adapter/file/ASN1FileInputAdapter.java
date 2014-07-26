/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2014.
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

package OpenRate.adapter.file;

import OpenRate.adapter.AbstractTransactionalInputAdapter;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.ASN1Exception;
import OpenRate.exception.EOCException;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

/**
 * File Reader for ASN.1 files
 *
 * @author Magnus
 */
public abstract class ASN1FileInputAdapter
  extends AbstractTransactionalInputAdapter
  implements IEventInterface
{
    enum inState {
        init,
        tag,
        length,
        value
    }

    /**
     * The reader used for managing the file
     */
    protected RandomAccessFile reader;

    /* ASN.1 Data Content */
    private int tag;
    private int length = -1;
    private byte[] value;

    private HashMap <Integer, String> asn1ClassName = new HashMap<>();

    /* "Local" variables */

    private int asn1_class;
    private inState state;
    private int id = 0;
    private long tagEndsAt = 0;
    private boolean constructed = false;
    private boolean application = false;
    private boolean atEOC = false;
    private boolean atEOF = false;

    private int UNIVERSAL_CLASS = 0x00;
    private int APPLICATION_CLASS = 0x01;
    private int CONTEXT_CLASS = 0x02;
    private int PRIVATE_CLASS = 0x03;

    /**
     * ASN1FileInput adapter encapsulates an ASN1 parser in the OpenRate
     * file processing structure.
     */
    public ASN1FileInputAdapter()
    {
    }

    private void init(String fileName) throws IOException
    {
        this.asn1ClassName.put(0x00, "Universal");
        this.asn1ClassName.put(0x01, "Application");
        this.asn1ClassName.put(0x02, "Context");
        this.asn1ClassName.put(0x03, "Application");

        try {
            reader = new RandomAccessFile(fileName, "rw");

            if (reader.length() == 0) throw new IOException();
        } catch (FileNotFoundException e) {
            throw new IOException(e.getMessage());
        } catch (IOException e) {
            throw e;
      }

      state = inState.init;
    }

   /**
    * Constructor for the ASN.1 file adapter
    *
    * @param fileName The name of the file we are processing
    * @throws IOException
    */
    public ASN1FileInputAdapter(String fileName) throws IOException
    {
        try {
            init(fileName);
        } catch (IOException ex) {
            throw ex;
        }
    }

    /**
     * Determines if we have finished reading the file
     *
     * @return true if we have not yet finished the file
     * @throws IOException
     */
    public boolean ready() throws IOException {
        try {
            return (reader.length() > reader.getFilePointer());
        } catch (IOException ex) {
            return false;
        }
    }

    /**
     * Close the file reader
     *
     * @throws IOException
     */
    public void close() throws IOException {
        try {
            reader.close();
        } catch (IOException ex) {
            throw ex;
        }
    }

    /**
     * Determine if the class is an application class
     *
     * @return true if the class is an application class
     */
    public boolean isApplication() {
        return (this.asn1_class == APPLICATION_CLASS);
    }

    /**
     * Determine if the class is a universal class
     *
     * @return true if the class is a universal class
     */
    public boolean isUniversal() {
        return (this.asn1_class == UNIVERSAL_CLASS);
    }

    /**
     * Determine if the class is a context class
     *
     * @return true if the class is a context class
     */
    public boolean isContext() {
        return (this.asn1_class == CONTEXT_CLASS);
    }

    /**
     * Determine if the class is a private class
     *
     * @return true if the class is a private class
     */
    public boolean isPrivate() {
        return (this.asn1_class == PRIVATE_CLASS);
    }

    /**
     * Get the class name of this class
     *
     * @return The class name of this class
     */
    public String getAsn1ClassName() {
        try {
            return asn1ClassName.get(this.asn1_class);
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * Get the class
     *
     * @return The class
     */
    public int getAsn1Class() {
        return asn1_class;
    }

    /**
     * Read the next tag in the file
     *
     * @return the next tag
     *
     * @throws EOFException
     * @throws EOCException
     * @throws IOException
     * @throws ASN1Exception
     */
    public int readTag() throws EOFException, EOCException, IOException, ASN1Exception
    {
        int TAG_MASK = 0x1F;        /* Bits 5 - 1 */
        int LEN_XTND = 0x80;        /* Indefinite or long form */
        int LEN_MASK = 0x7F;        /* Bits 7 - 1 */
        int EOC=0x00;               /* End of content Octet */

        /* Encoding type */
        int FORM_MASK =         0x20;       /* Bit 6 */
        int CONSTRUCTED =       0x20;

        /* Local variables */
        int readByte;

        if (state != inState.init && state != inState.value ) throw new ASN1Exception("In wrong state");

        try
        {
          readByte = reader.readByte();

          if(readByte == EOC) {
            atEOC = true;
            throw new EOCException("End of block found");
          }

          readByte &= 0xff; // Make it unsigned

          this.id = readByte & ~TAG_MASK;
          this.constructed = ((id & FORM_MASK) == CONSTRUCTED ); // is the tag constructed or primitive?
          this.asn1_class = (id & (APPLICATION_CLASS << 6)) >> 6;  // is the type application specific?
          this.application = ((id & (APPLICATION_CLASS << 6)) == (APPLICATION_CLASS << 6));

          readByte &= TAG_MASK;
          this.tag = readByte;

          if( readByte == TAG_MASK )
          {
            int inValue;

            /* Long tag encoded as sequence of 7-bit values.  This doesn't try to
               handle tags > INT_MAX, it'd be pretty peculiar ASN.1 if it had to
               use tags this large */
            readByte = 0;
            do
            {
                inValue = reader.readByte() & 0xFF;
                readByte = (int) ((readByte << 7) | (inValue & 0x7F));
                if ( (inValue>>7) == 0 )
                {
                    break;
                }
            } while( ((inValue & LEN_XTND) != 0) );

          }
          this.tag = readByte;
        } catch (EOFException e) {
            atEOF = true;
                throw e;
        } catch (IOException e) {
                throw e;
        }
        state = inState.tag;
        return this.tag;
  }

    /**
     * Read the length of the tag
     *
     * @return The length
     * @throws IOException
     * @throws ASN1Exception
     */
    public int readLength() throws IOException, ASN1Exception
  {
    int LEN_XTND = 0x80;        /* Indefinite or long form */
    int LEN_MASK = 0x7F;        /* Bits 7 - 1 */

    /* Local variables */
    int inLength;

    if (state != inState.tag ) throw new ASN1Exception("In wrong state");

    try {
        inLength = reader.readByte();
    } catch (EOFException e) {
        return -1;
    } catch (IOException e) {
        throw e;
    }

    // Indefinite length
    if ((inLength & LEN_MASK) == 0x00)
    {
        inLength = 128;
    }
    else if ((inLength & LEN_MASK) != inLength)
    {
        // This is a multibyte length.  Find the actual length
        int numLengthBytes = (inLength & LEN_MASK);
        inLength = 0x00000000;
        byte[] buffer = new byte[numLengthBytes];
        try {
            if (reader.read(buffer) == -1) throw new EOFException("At end of file");
        } catch (IOException e) {
            throw e;
        }

        switch (numLengthBytes)
        {
            case 1:   inLength |= (0x000000FF & buffer[0]);
                    break;
            case 2:   inLength |= ((0x000000FF & buffer[0]) << 8) |
                              (0x000000FF & buffer[1]);
                    break;
            case 3:   inLength |= ((0x000000FF & buffer[0]) << 16) |
                              ((0x000000FF & buffer[1]) << 8) |
                              (0x000000FF & buffer[2]);
                    break;
            case 4:   inLength |= ((0x000000FF & buffer[0]) << 24) |
                              ((0x000000FF & buffer[1]) << 16) |
                              ((0x000000FF & buffer[2]) << 8) |
                              (0x000000FF & buffer[3]);
                    break;
            default:  throw new ASN1Exception("Length cannot be represented as " +
                                            "a Java int");
        }
    }
    state = inState.length;
    this.tagEndsAt = inLength + reader.getFilePointer();
    this.length = inLength;
    return inLength;
  }

    /**
     * Read the value of a tag
     *
     * @return Byte array of the value
     * @throws IOException
     * @throws ASN1Exception
     */
    public byte[] readValue()  throws IOException, ASN1Exception
  {
    try {
        return readValue(this.length);
    } catch (IOException | ASN1Exception e) {
        throw e;
    }
  }

    /**
     * Read the value of a definite length tag
     *
     * @param length The length to read
     * @return Byte array of the value
     * @throws IOException
     * @throws ASN1Exception
     */
    public byte[] readValue(int length) throws IOException, ASN1Exception
  {
    if(state != inState.length) throw new ASN1Exception("In wrong state");

    if (!constructed)
    {
        byte[] inByte = new byte[length];

        try {
            if (reader.read(inByte) != -1) {
                state = inState.value;
                this.value = inByte;
                return inByte;
            }
        } catch (EOFException e) {
          atEOF = true;
          throw new IOException(e.getMessage());
        } catch (IOException e) {
          throw e;
        }
    }
    state = inState.value;
    this.value = null;
    return null;
  }

    /**
     * Get the end of the tag
     *
     * @return The end of the tag
     * @throws ASN1Exception
     */
    public long getValueEndsAt() throws ASN1Exception {
      if (state == inState.tag || state == inState.value) {
        return tagEndsAt;
      } else {
        throw new ASN1Exception("In wrong state");
      }
  }

    /**
     * Determine if the state of the ASN parser
     *
     * @return true if the parser is constructed
     * @throws ASN1Exception
     */
    public boolean isConstructed() throws ASN1Exception {
      if (state != inState.init) {
        return constructed;
      } else {
        throw new ASN1Exception("In wrong state");
      }
  }

    /**
     * Get the file pointer for the current reader
     *
     * @return The file pointer
     * @throws IOException
     */
    public long getFilePointer() throws IOException {
      try {
        return reader.getFilePointer();
      } catch (IOException e) {
          throw e;
      }
  }

}