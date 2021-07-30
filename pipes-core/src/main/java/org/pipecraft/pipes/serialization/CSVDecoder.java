package org.pipecraft.pipes.serialization;

import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.pipes.exceptions.ValidationPipeException;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * An {@link ItemDecoder} that decodes CSV rows.
 * Supports skipping header rows.
 * Uses user defined function to create the java object from the string array parsed from an input line.
 * 
 * @author Eyal Schneider
 */
public class CSVDecoder<T> implements ItemDecoder<T> {
  private static final char DEFAULT_DELIMITER = ',';
  private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
  private final BufferedReader reader;
  private final CsvParser csvReader;
  private final FailableFunction<String[], T, ValidationPipeException> itemDetextualizer;

  /**
   * Constructor
   *
   * @param is The input stream this decoder is bound to. Not expected to be buffered. Buffering is added internally.
   * @param itemDetextualizer The logic that transforms a parsed line (String[]) into the corresponding java object.
   *                          A column which is empty in the input is passed as null, unless it's an empty quoted string, which is passed as an empty string.
   *                          Leading/trailing spaces from the input are kept (even if not quoted), so it's up to this function to decide what to do with them.
   * @param charset The character encoding to assume when reading from the input stream
   * @param delimiter The CSV delimiter character to use
   * @param rowsToSkip The number of header rows to skip before parsing data lines. Non-negative.
   * @param readOptions Input reading options (buffering/compression)
   * @throws IOException In case of an IO error while preparing to read
   */
  public CSVDecoder(InputStream is, FailableFunction<String[], T, ValidationPipeException> itemDetextualizer, Charset charset, char delimiter, int rowsToSkip, FileReadOptions readOptions) throws IOException {
    this.itemDetextualizer = itemDetextualizer;
    CsvParserSettings settings = new CsvParserSettings();
    settings.setIgnoreLeadingWhitespaces(false);
    settings.setIgnoreTrailingWhitespaces(false);
    settings.setMaxCharsPerColumn(-1); // No limit on line size
    settings.setNumberOfRowsToSkip(rowsToSkip);
    settings.setEmptyValue("");
//    settings.setNullValue();
    CsvFormat format = settings.getFormat();
    format.setDelimiter(delimiter);
    format.setLineSeparator("\n");
    this.reader = FileUtils.getReader(is, charset, readOptions);
    this.csvReader = new CsvParser(settings);
    csvReader.beginParsing(reader);
  }

  /**
   * Constructor
   *
   * Assumes UTF8, comma as a delimiter and default read options (no compression)
   * @param is The input stream this decoder is bound to. Not expected to be buffered. Buffering is added internally.
   * @param itemDetextualizer The logic that transforms a parsed line (String[]) into the corresponding java object.
   *                          Columns which are empty in the input are passes as null. Leading/trailing spaces from the input are kept, so
   *                          it's up to this function to decide what to do with them.
   * @param rowsToSkip The number of header rows to skip before parsing data lines. Non-negative.
   * @throws IOException In case of an IO error while preparing to read
   */
  public CSVDecoder(InputStream is, FailableFunction<String[], T, ValidationPipeException> itemDetextualizer, int rowsToSkip) throws IOException {
    this(is, itemDetextualizer, DEFAULT_CHARSET, DEFAULT_DELIMITER, rowsToSkip, new FileReadOptions());
  }

  /**
   * Constructor
   *
   * Assumes UTF8, comma as a delimiter, no header rows and default read options (no compression)
   * @param is The input stream this decoder is bound to. Not expected to be buffered. Buffering is added internally.
   * @param itemDetextualizer The logic that transforms a parsed line (String[]) into the corresponding java object.
   *                          Columns which are empty in the input are passes as null. Leading/trailing spaces from the input are kept, so
   *                          it's up to this function to decide what to do with them.
   * @throws IOException In case of an IO error while preparing to read
   */
  public CSVDecoder(InputStream is, FailableFunction<String[], T, ValidationPipeException> itemDetextualizer) throws IOException {
    this(is, itemDetextualizer, DEFAULT_CHARSET, DEFAULT_DELIMITER, 0, new FileReadOptions());
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public T decode() throws IOException, ValidationPipeException {
    String[] line = csvReader.parseNext();
    if (line == null) {
      return null;
    }
    return itemDetextualizer.apply(line);
  }

  /**
   * @param itemDetextualizer The logic that transforms a parsed line (String[]) into the corresponding java object.
   *                          Columns which are empty in the input are passes as null. Leading/trailing spaces from the input are kept, so
   *                          it's up to this function to decide what to do with them.
   * @param charset The character encoding to assume when reading from the input stream
   * @param delimiter The CSV delimiter character to use
   * @param rowsToSkip The number of header rows to skip before parsing data lines. Non-negative.
   * @return A decoder factory producing CSV decoders based on the given settings
   */
  public static <R> DecoderFactory<R> getFactory(FailableFunction<String[], R, ValidationPipeException> itemDetextualizer, Charset charset, char delimiter, int rowsToSkip) {
    return new Factory<>(itemDetextualizer, charset, delimiter, rowsToSkip);
  }

  /**
   * @param itemDetextualizer The logic that transforms a parsed line (String[]) into the corresponding java object.
   *                          Columns which are empty in the input are passes as null. Leading/trailing spaces from the input are kept, so
   *                          it's up to this function to decide what to do with them.
   * @return A decoder factory producing CSV decoders based on the given settings. Assumes UTF8, comma as delimiter and no header rows.
   */
  public static <R> DecoderFactory<R> getFactory(FailableFunction<String[], R, ValidationPipeException> itemDetextualizer) {
    return new Factory<>(itemDetextualizer, DEFAULT_CHARSET, DEFAULT_DELIMITER, 0);
  }

  private static class Factory <R> implements DecoderFactory<R> {
    private final FailableFunction<String[], R, ValidationPipeException> itemDetextualizer;
    private final Charset charset;
    private final char delimiter;
    private final int rowsToSkip;

    public Factory(FailableFunction<String[], R, ValidationPipeException> itemDetextualizer, Charset charset, char delimiter, int rowsToSkip) {
      this.itemDetextualizer = itemDetextualizer;
      this.charset = charset;
      this.delimiter = delimiter;
      this.rowsToSkip = rowsToSkip;
    }

    @Override
    public ItemDecoder<R> newDecoder(InputStream is, FileReadOptions options) throws IOException {
      return new CSVDecoder<>(is, itemDetextualizer, charset, delimiter, rowsToSkip, options);
    }
  }

}
