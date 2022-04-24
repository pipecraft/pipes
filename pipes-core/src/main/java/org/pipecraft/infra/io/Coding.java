package org.pipecraft.infra.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.nio.charset.StandardCharsets;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * General encoding/decoding utility functions
 * 
 * @author Roman Gershman, Eyal Schneider
 */
public final class Coding {
  
  /**
   * Reads a given number of bytes from the stream into a new buffer
   * @param is The input stream
   * @param bytesCount The number of bytes to read from the stream
   * @return The byte array that has been read
   * @throws IOException In case of a read error
   * @throws EOFException if the stream doesn't contain the requested number of bytes
   */
  public static byte[] read(InputStream is, int bytesCount) throws IOException {    
    byte[] buffer = new byte[bytesCount];
    IOUtils.readFully(is, buffer, 0, bytesCount);
    return buffer;
  }

  /**
   * Reads a given number of bytes from the stream into a given buffer
   * @param is The input stream
   * @param buffer The buffer to read into
   * @param offset The offset in the given buffer to write from
   * @param bytesCount The number of bytes to read from the stream
   * @throws IOException In case of a read error
   * @throws EOFException if the stream doesn't contain the requested number of bytes
   */
  public static void read(InputStream is, byte[] buffer, int offset, int bytesCount) throws IOException {
    IOUtils.readFully(is, buffer, offset, bytesCount);
  }

  /**
   * Tries to read a given number of bytes from the stream into a given buffer.
   * If EOF is encountered, returns false to indicate it. However, if only part of the expected
   * bytes are available, throws EOFException.
   * @param is The input stream
   * @param buffer The buffer to read into
   * @param offset The offset in the given buffer to write from
   * @param bytesCount The number of bytes to try to read from the stream
   * @return true if the buffer has been filled, and false if the input stream has no bytes to read at all
   * @throws IOException In case of a read error
   * @throws EOFException if the stream isn't in EOF state prior to the call, and doesn't contain requested number of bytes either.
   */
  public static boolean tryRead(InputStream is, byte[] buffer, int offset, int bytesCount) throws IOException {
    int bytesRead = is.readNBytes(buffer, offset, bytesCount);
    if (bytesRead == 0) {
      return false;
    }
    if (bytesRead < bytesCount) {
      throw new EOFException("Expected " + bytesCount + " bytes but got " + bytesRead);
    }
    return true;
  }

  /**
   * Reads a single mandatory byte from a given stream
   * @param is The input stream to read from
   * @return The read byte
   * @throws IOException in case of IO error
   * @throws EOFException if the stream doesn't contain any more bytes
   */
  public static byte read(InputStream is) throws IOException {
    int v = is.read();
    if (v == -1) {
      throw new EOFException("Stream ended, but expected at least one more byte.");
    }
    return (byte) v;
  }

  // --------------------------- LE 16 bit ---------------------------------------
  
  /**
   * Reads a 16 bit int from the input stream. LSB are assumed to comes first.
   * @param is The input stream to read from
   * @return The 16 bit int read from the input stream
   * @throws IOException In case of read error
   * @throws EOFException if the stream doesn't contain the requested number of bytes
   */
  public static int readLittleEndian16(InputStream is) throws IOException {
    byte[] buffer = read(is, 2);
    return readLittleEndian16(buffer, 0);
  }

  public static int readLittleEndian16(final byte[] src, int offset) {
    return ((src[offset + 0] & 0xff)) |
           ((src[offset + 1] & 0xff) <<  8);
  }

  public static byte[] getLittleEndian16AsBytes(final int value) {
    byte[] res = new byte[2];
    writeLittleEndian16(value, res, 0);
    return res;
  }

  /**
   * Writes a 16 bit int into the output stream. LSB comes first.
   * @param v The value to write
   * @param os the output stream to write to
   * @throws IOException  In case of write error
   */
  public static void writeLittleEndian16(int v, OutputStream os) throws IOException {
    os.write(v);
    os.write(v >> 8);
  }

  public static void writeLittleEndian16(final int value, byte[] dest, int offset) {
    dest[offset++] = (byte) ((value) & 0xFF);
    dest[offset] = (byte) ((value >> 8) & 0xFF);
  }

  //--------------------------- BE 16 bit ---------------------------------------
  
  /**
   * Reads a 16 bit int from the input stream. MSB are assumed to comes first.
   * @param is The input stream to read from
   * @return The 16 bit int read from the input stream
   * @throws IOException In case of read error
   * @throws EOFException if the stream doesn't contain the requested number of bytes
   */
  public static int readBigEndian16(InputStream is) throws IOException {
    byte[] buffer = read(is, 2);
    return readBigEndian16(buffer, 0);
  }

  public static int readBigEndian16(final byte[] src, int offset) {
    return ((src[offset + 1] & 0xff)) |
           ((src[offset + 0] & 0xff) <<  8);
  }

  public static byte[] getBigEndian16AsBytes(final int value) {
    byte[] res = new byte[2];
    writeBigEndian16(value, res, 0);
    return res;
  }

  /**
   * Writes a 16 bit int into the output stream. MSB comes first.
   * @param v The value to write
   * @param os the output stream to write to
   * @throws IOException In case of write error
   */
  public static void writeBigEndian16(int v, OutputStream os) throws IOException {
    os.write(v >> 8);
    os.write(v);
  }

  public static void writeBigEndian16(final int value, byte[] dest, int offset) {
    dest[offset++] = (byte) ((value >> 8) & 0xFF);
    dest[offset] = (byte) ((value) & 0xFF);
  }

  //--------------------------- LE 32 bit ---------------------------------------

  /**
   * Reads a 32 bit int from the input stream. LSB are assumed to comes first.
   * @param is The input stream to read from
   * @return The 32 bit int read from the input stream
   * @throws IOException In case of read error
   * @throws EOFException if the stream doesn't contain the requested number of bytes
   */
  public static int readLittleEndian32(InputStream is) throws IOException {
    byte[] buffer = read(is, 4);
    return readLittleEndian32(buffer, 0);
  }
  
  public static int readLittleEndian32(final byte[] src, int offset) {
    return ((src[offset + 0] & 0xff)) |
           ((src[offset + 1] & 0xff) <<  8) |
           ((src[offset + 2] & 0xff) << 16) |
           ((src[offset + 3] & 0xff) << 24);
  }

  public static byte[] getLittleEndian32AsBytes(final int value) {
    byte[] res = new byte[4];
    writeLittleEndian32(value, res, 0);
    return res;
  }

  /**
   * Writes a 32 bit int into the output stream. LSB comes first.
   * @param v The value to write
   * @param os the output stream to write to
   * @throws IOException In case of write error
   */
  public static void writeLittleEndian32(int v, OutputStream os) throws IOException {
    os.write(v);
    os.write(v >> 8);
    os.write(v >> 16);
    os.write(v >> 24);
  }

  public static void writeLittleEndian32(final int value, byte[] dest, int offset) {
    dest[offset++] = (byte) ((value) & 0xFF);
    dest[offset++] = (byte) ((value >> 8) & 0xFF);
    dest[offset++] = (byte) ((value >> 16) & 0xFF);
    dest[offset] = (byte) ((value >> 24) & 0xFF);
  }

  //--------------------------- BE 32 bit ---------------------------------------

  /**
   * Reads a 32 bit int from the input stream. MSB are assumed to comes first.
   * @param is The input stream to read from
   * @return The 32 bit int read from the input stream
   * @throws IOException In case of read error
   * @throws EOFException if the stream doesn't contain the requested number of bytes
   */
  public static int readBigEndian32(InputStream is) throws IOException {
    byte[] buffer = read(is, 4);
    return readBigEndian32(buffer, 0);
  }

  public static int readBigEndian32(final byte[] src, int offset) {
    return ((src[offset + 0] & 0xff) << 24) |
           ((src[offset + 1] & 0xff) << 16) |
           ((src[offset + 2] & 0xff) <<  8) |
           ((src[offset + 3] & 0xff));
  }

  public static byte[] getBigEndian32AsBytes(final int value) {
    byte[] res = new byte[4];
    writeBigEndian32(value, res, 0);
    return res;
  }

  /**
   * Writes a 32 bit int into the output stream. MSB comes first.
   * @param v The value to write
   * @param os the output stream to write to
   * @throws IOException In case of write error
   */
  public static void writeBigEndian32(int v, OutputStream os) throws IOException {
    os.write(v >> 24);
    os.write(v >> 16);
    os.write(v >> 8);
    os.write(v);
  }

  public static void writeBigEndian32(final int value, byte[] dest, int offset) {
    dest[offset++] = (byte) ((value >> 24) & 0xFF);
    dest[offset++] = (byte) ((value >> 16) & 0xFF);
    dest[offset++] = (byte) ((value >> 8) & 0xFF);
    dest[offset] = (byte) ((value) & 0xFF);
  }

  //--------------------------- LE 64 bit ---------------------------------------

  /**
   * Reads a 64 bit int from the input stream. LSB are assumed to comes first.
   * @param is The input stream to read from
   * @return The 64 bit int read from the input stream
   * @throws IOException In case of read error
   * @throws EOFException if the stream doesn't contain the requested number of bytes
   */
  public static long readLittleEndian64(InputStream is) throws IOException {
    byte[] buffer = read(is, 8);
    return readLittleEndian64(buffer, 0);
  }

  public static long readLittleEndian64(final byte[] src, int offset) {
    return (long)(src[offset + 0] & 0xff)       |
           (long)(src[offset + 1] & 0xff) <<  8 |
           (long)(src[offset + 2] & 0xff) << 16 |
           (long)(src[offset + 3] & 0xff) << 24 |
           (long)(src[offset + 4] & 0xff) << 32 |
           (long)(src[offset + 5] & 0xff) << 40 |
           (long)(src[offset + 6] & 0xff) << 48 |
           (long)(src[offset + 7] & 0xff) << 56;
  }

  public static byte[] getLittleEndian64AsBytes(final long value) {
    byte[] res = new byte[8];
    writeLittleEndian64(value, res, 0);
    return res;
  }

  /**
   * Writes a 64 bit int into the output stream. LSB comes first.
   * @param v The value to write
   * @param os the output stream to write to
   * @throws IOException In case of write error
   */
  public static void writeLittleEndian64(long v, OutputStream os) throws IOException {
    os.write((int) v);
    os.write((int) (v >> 8));
    os.write((int) (v >> 16));
    os.write((int) (v >> 24));
    os.write((int) (v >> 32));
    os.write((int) (v >> 40));
    os.write((int) (v >> 48));
    os.write((int) (v >> 56));
  }

  public static void writeLittleEndian64(final long value, byte[] dest, int offset) {
    dest[offset++] = (byte) (value & 0xFF);
    dest[offset++] = (byte) (value >> 8 & 0xFF);
    dest[offset++] = (byte) (value >> 16 & 0xFF);
    dest[offset++] = (byte) (value >> 24 & 0xFF);
    dest[offset++] = (byte) (value >> 32 & 0xFF);
    dest[offset++] = (byte) (value >> 40 & 0xFF);
    dest[offset++] = (byte) (value >> 48 & 0xFF);
    dest[offset]   = (byte) (value >> 56 & 0xFF);
  }

  //--------------------------- BE 64 bit ---------------------------------------

  /**
   * Reads a 64 bit int from the input stream. MSB are assumed to comes first.
   * @param is The input stream to read from
   * @return The 64 bit long read from the input stream
   * @throws IOException In case of IO error
   * @throws EOFException if the stream doesn't contain the requested number of bytes
   */
  public static long readBigEndian64(InputStream is) throws IOException {
    byte[] buffer = read(is, 8);
    return readBigEndian64(buffer, 0);
  }

  public static long readBigEndian64(final byte[] src, int offset) {
    return (long)(src[offset + 0] & 0xff) << 56 |
           (long)(src[offset + 1] & 0xff) << 48 |
           (long)(src[offset + 2] & 0xff) << 40 |
           (long)(src[offset + 3] & 0xff) << 32 |
           (long)(src[offset + 4] & 0xff) << 24 |
           (long)(src[offset + 5] & 0xff) << 16 |
           (long)(src[offset + 6] & 0xff) << 8  |
           (long)(src[offset + 7] & 0xff);
  }

  public static byte[] getBigEndian64AsBytes(final long value) {
    byte[] res = new byte[8];
    writeBigEndian64(value, res, 0);
    return res;
  }

  /**
   * Writes a 64 bit int into the output stream. MSB comes first.
   * @param v The value to write
   * @param os the output stream to write to
   * @throws IOException in case of a write error
   */
  public static void writeBigEndian64(long v, OutputStream os) throws IOException {
    os.write((int) (v >> 56));
    os.write((int) (v >> 48));
    os.write((int) (v >> 40));
    os.write((int) (v >> 32));
    os.write((int) (v >> 24));
    os.write((int) (v >> 16));
    os.write((int) (v >> 8));
    os.write((int) v);
  }  

  public static void writeBigEndian64(final long value, byte[] dest, int offset) {
    dest[offset++]   = (byte) (value >> 56 & 0xFF);
    dest[offset++] = (byte) (value >> 48 & 0xFF);
    dest[offset++] = (byte) (value >> 40 & 0xFF);
    dest[offset++] = (byte) (value >> 32 & 0xFF);
    dest[offset++] = (byte) (value >> 24 & 0xFF);
    dest[offset++] = (byte) (value >> 16 & 0xFF);
    dest[offset++] = (byte) (value >> 8 & 0xFF);
    dest[offset] = (byte) (value & 0xFF);
  }

  /* Strings */

  /**
   * @param s The string to serialize
   * @param os The output stream to write to
   * @throws IOException in case of a write error
   */
  public static void writeUTF8(String s, OutputStream os) throws IOException {
    byte[] byteArr = s.getBytes(StandardCharsets.UTF_8);
    Coding.writeVarint32(byteArr.length, os);
    os.write(byteArr);
  }

  /**
   * @param is The input stream to read from
   * @return The string read from the stream
   * @throws IOException in case of a read error
   * @throws EOFException if the stream doesn't contain the requested number of bytes
   */
  public static String readUTF8(InputStream is) throws IOException {
    byte[] byteArr = read(is, readVarint32(is, new MutableInt()));
    return new String(byteArr, StandardCharsets.UTF_8);
  }

  /* VARINT32 routines  The following functions are adapted from protobuf package. */
  
  /**
   * @param value The value to inspect the varint encoding for. Treated as unsigned, so it won't be sign-extended if
   * negative.
   * @return the number of bytes that would be needed to encode a varint.
   */
  public static byte computeVarint32Size(final int value) {
    if ((value & (0xffffffff <<  7)) == 0) return 1;
    if ((value & (0xffffffff << 14)) == 0) return 2;
    if ((value & (0xffffffff << 21)) == 0) return 3;
    if ((value & (0xffffffff << 28)) == 0) return 4;
    return 5;
  }
  
  
  /**
   * @param value The value to encode
   * @param dest The byte array to write the encoding to
   * @param offset The offset in dest to start writing the varint from
   * @return new offset to the dest after the value was written.
   */
  public static int writeVarint32(int value, byte[] dest, int offset) {
    while (true) {
      if ((value & ~0x7F) == 0) {
        dest[offset++] = (byte) value;
        return offset;
      }
      dest[offset++] = (byte)((value & 0x7F) | 0x80);      
      value >>>= 7;
    }
  }

  /**
   * @param value The value to write
   * @param os The stream to write to. Not closed by this method.
   * @return the number of bytes written
   * @throws IOException in case of a write error
   */
  public static int writeVarint32(int value, OutputStream os) throws IOException {
    byte[] buffer = new byte[5];
    int count = writeVarint32(value, buffer, 0);
    os.write(buffer, 0, count);
    return count;
  }

  /**
   * Reads a varint from a given input stream.
   * @param is The input stream to read from
   * @param length Output parameter - The number of bytes composing the varint
   * @return the varint value
   * @throws IOException in case of an illegal varint encoding or IO error while reading from stream
   */
  public static int readVarint32(InputStream is, MutableInt length) throws IOException {
    int res = 0;
    int count = 0;
    while(true) {
      int value = is.read();
      if (value == -1) {
        throw new IOException("Failed reading a varint. Expected more bytes.");
      }
      res |= (value & ~0x80) << (7 * count++);
      if ((value & 0x80) == 0) { // Last byte
        break;
      }
      if (count == 5) {
        throw new IOException("Illegal varint. Length is greater than 5.");
      }
    }
    length.setValue(count);
    return res;
  }
  
  /**
   * Reads a varint from a given byte array
   * @param src The array to read from
   * @param offset The offset to read from. This value is updated according to the actual number of bytes read.
   * @return the varint value
   * @throws IOException in case of an illegal varint encoding
   */
  public static int readVarint32(byte[] src, MutableInt offset) throws IOException {
    int res = 0;
    int count = 0;
    int pos = offset.intValue();
    while(true) {
      if (pos == src.length) {
        throw new IOException("Failed reading a varint. Expected more bytes.");
      }

      int value = Byte.toUnsignedInt(src[pos++]);
      res |= (value & ~0x80) << (7 * count++);
      if ((value & 0x80) == 0) { // Last byte
        break;
      }
      if (count == 5) {
        throw new IOException("Illegal varint. Length is greater than 5.");
      }
    }
    offset.add(count);
    return res;
  }

  /**
   * convert hex to byte array
   * @param hex The hex representation, case insensitive
   * @return hex as byte array
   * @throws DecoderException in case of an invalid hex string
   */
  public static byte[] fromHex(String hex) throws DecoderException {
    return Hex.decodeHex(hex.toCharArray());
  }

  /**
   * convert byte array to hex representation
   * @param raw The byte array to encode as hex string
   * @return The hex representation
   */
  public static String toHex(byte[] raw) {
    return Hex.encodeHexString(raw);
  }
}
