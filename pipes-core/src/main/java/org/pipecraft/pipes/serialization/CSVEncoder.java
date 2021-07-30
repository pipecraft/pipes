package org.pipecraft.pipes.serialization;

import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * An {@link ItemEncoder} that encodes objects as compliant CSV rows.
 * Supports producing header row/s.
 * Uses user defined function to encode object fields as a string array.
 *
 * @author Eyal Schneider
 */
public class CSVEncoder<T> implements ItemEncoder<T> {
  private static final char DEFAULT_DELIMITER = ',';
  private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
  private final CsvWriter csvWriter;
  private final Function<T, String[]> itemTextualizer;

  /**
   * Constructor
   *
   * @param os The output stream this encoder is bound to. Not expected to be buffered. Buffering is added internally.
   * @param itemTextualizer The logic that transforms an item into the corresponding CSV columns.
   *                        Note that this function is responsible for handling leading/trailing spaces, since they are passed to output unchanged.
   *                        Also note that an empty string will be written as an empty quoted string, to distinguish it from an empty column which is read as null.
   * @param charset The character encoding to use when writing to the output stream
   * @param delimiter The CSV delimiter character to use
   * @param headerBlock A text to be written to the output, once, at the beginning.
   *                    Use '\n' to create multiple lines. Use null to add no header block.
   * @param writeOptions output writing options (buffering/compression)
   * @throws IOException In case of an IO error while preparing to write
   */
  public CSVEncoder(OutputStream os, Function<T, String[]> itemTextualizer, Charset charset, char delimiter, String headerBlock
      , FileWriteOptions writeOptions) throws IOException {
    this.itemTextualizer = itemTextualizer;
    CsvWriterSettings settings = new CsvWriterSettings();
    settings.setIgnoreLeadingWhitespaces(false);
    settings.setIgnoreTrailingWhitespaces(false);
    settings.setMaxCharsPerColumn(-1); // No limit on line size
    settings.setEmptyValue("\"\"");
    CsvFormat format = settings.getFormat();
    format.setDelimiter(delimiter);
    format.setLineSeparator("\n");
    BufferedWriter writer = FileUtils.getWriter(os, charset, writeOptions);
    if (headerBlock != null) {
      writer.write(headerBlock);
      writer.write('\n');
    }
    this.csvWriter = new CsvWriter(writer, settings);
  }

  /**
   * Constructor
   *
   * Assumes UTF8, comma as a delimiter and default write options (no compression)
   * @param os The output stream this encoder is bound to. Not expected to be buffered. Buffering is added internally.
   * @param itemTextualizer The logic that transforms an item into the corresponding CSV columns.
   *                        Note that this function is responsible for handling leading/trailing spaces, since they are passed to output unchanged.
   * @throws IOException In case of an IO error while preparing to write
   */
  public CSVEncoder(OutputStream os, Function<T, String[]> itemTextualizer) throws IOException {
    this(os, itemTextualizer, DEFAULT_CHARSET, DEFAULT_DELIMITER, null, new FileWriteOptions());
  }

  @Override
  public void close() throws IOException {
    csvWriter.flush();
    csvWriter.close();
  }

  @Override
  public void encode(T item) throws IOException {
    String[] columns = itemTextualizer.apply(item);
    csvWriter.writeRow(columns);
  }

  /**
   * @param itemTextualizer The logic that transforms an item into the corresponding CSV columns.
   *                        Note that this function is responsible for handling leading/trailing spaces, since they are passed to output unchanged.
   * @param charset The character encoding to use when writing to the output stream
   * @param delimiter The CSV delimiter character to use
   * @param headerBlock A text to be written to the output, once, at the beginning.
   *                    Use '\n' to create multiple lines. Use null to add no header block.
   * @return An encoder factory producing CSV encoders based on the given settings
   */
  public static <R> EncoderFactory<R> getFactory(Function<R, String[]> itemTextualizer, Charset charset, char delimiter, String headerBlock) {
    return new Factory<>(itemTextualizer, charset, delimiter, headerBlock);
  }

  /**
   * @param itemTextualizer The logic that transforms an item into the corresponding CSV columns.
   *                        Note that this function is responsible for handling leading/trailing spaces, since they are passed to output unchanged.
   * @return An encoder factory producing CSV encoders based on the given settings. Assumes UTF 8, comma as a CSV delimiter, and no header.
   */
  public static <R> EncoderFactory<R> getFactory(Function<R, String[]> itemTextualizer) {
    return new Factory<>(itemTextualizer, DEFAULT_CHARSET, DEFAULT_DELIMITER, null);
  }

  private static class Factory <R> implements EncoderFactory<R> {
    private final Function<R, String[]> itemTextualizer;
    private final Charset charset;
    private final char delimiter;
    private final String headerBlock;

    public Factory(Function<R, String[]> itemTextualizer, Charset charset, char delimiter, String headerBlock) {
      this.itemTextualizer = itemTextualizer;
      this.charset = charset;
      this.delimiter = delimiter;
      this.headerBlock = headerBlock;
    }

    @Override
    public ItemEncoder<R> newEncoder(OutputStream os, FileWriteOptions options) throws IOException {
      return new CSVEncoder<>(os, itemTextualizer, charset, delimiter, headerBlock, options);
    }
  }

}
