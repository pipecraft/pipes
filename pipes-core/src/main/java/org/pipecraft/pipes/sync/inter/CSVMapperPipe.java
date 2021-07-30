package org.pipecraft.pipes.sync.inter;

import com.opencsv.CSVParserBuilder;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.opencsv.CSVParser;
import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.concurrent.FailableFunction;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.exceptions.ValidationPipeException;

/**
 * An intermediate pipe that parses strings in CSV format, and binds each line to an object
 *
 * @param <T> The type of the object we wish to bind the CSV lines to
 *
 * @author Eyal Schneider
 */
public class CSVMapperPipe<T> implements Pipe<T> {
  private static final char DEFAULT_DELIMITER = ',';

  private final Pipe<String> input;
  private final boolean hasHeader;
  private volatile String[] columnNames;
  private final FailableFunction<Map<String, String>, T, PipeException> rowConverter;
  private T next;
  private final CSVParser csvParser;

  /**
   * Constructor
   *
   * @param input The input pipe
   * @param delimiter The delimiter character to use
   * @param hasHeader When true, the first item from the input pipe will be processed as a header
   * @param columnNames Describes the columns in the input CSV, according to their expected order. These names will be the column names passed to the row converter.
   * Note that even when hasHeader=true, this parameter can be passed in order to internally "rename" column names. This guarantees that we only see expected names
   * in the row converter. When null, we rely completely on the header, and column names will be accordingly. Null isn't allowed when hasHeader=false.  
   * @param rowConverter Converts a tokenized row into the target row object. May throw a {@link PipeException} in case of a row which can't be handled.
   */
  public CSVMapperPipe(Pipe<String> input, char delimiter, boolean hasHeader, String[] columnNames, FailableFunction<Map<String, String>, T, PipeException> rowConverter) {
    if (columnNames == null && !hasHeader) {
      throw new IllegalArgumentException("Must eather specify column names, or require a header in the input pipe");
    }

    this.input = input;
    this.hasHeader = hasHeader;    
    this.csvParser = new CSVParserBuilder().withSeparator(delimiter).build();
    this.columnNames = columnNames;
    this.rowConverter = rowConverter;
  }

  /**
   * Constructor
   *
   * Constructs a mapper with the default delimiter (',').
   * 
   * @param input The input pipe
   * @param hasHeader When true, the first item from the input pipe will be processed as a header
   * @param columnNames Describes the columns in the input CSV, according to their expected order. These names will be the column names passed to the row converter.
   * Note that even when hasHeader=true, this parameter can be passed in order to internally "rename" column names. This guarantees that we only see expected names
   * in the row converter. When null, we rely completely on the header, and column names will be accordingly. Null isn't allowed when hasHeader=false.  
   * @param rowConverter Converts a tokenized row into the target row object. May throw a {@link PipeException} in case of a row which can't be handled.
   */
  public CSVMapperPipe(Pipe<String> input, boolean hasHeader, String[] columnNames, FailableFunction<Map<String, String>, T, PipeException> rowConverter) {
    this(input, DEFAULT_DELIMITER, hasHeader, columnNames, rowConverter);
  }

  /**
   * Constructor
   *
   * Constructs a mapper with the default delimiter (','), expecting a header, and using the header column names for the row converter.
   * 
   * @param input The input pipe
   * @param rowConverter Converts a tokenized row into the target row object. May throw a {@link PipeException} in case of a row which can't be handled.
   */
  public CSVMapperPipe(Pipe<String> input, FailableFunction<Map<String, String>, T, PipeException> rowConverter) {
    this(input, DEFAULT_DELIMITER, true, null, rowConverter);
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();
    
    // Determine column names
    if (hasHeader) {
      String header = input.next();
      if (header == null) {
        throw new ValidationPipeException("Expected CSV header");
      }
      String[] headerColumns = splitRow(header);
      if (columnNames == null) { // No internal names provided - use the names in the header
        columnNames = headerColumns;
      } else { // Internal names will be used, provided that they match the header size.
        if (columnNames.length != headerColumns.length) {
          throw new ValidationPipeException("Number of columns in input is " + headerColumns.length + ", but expected " + columnNames.length);
        }
      }
    } 
    
    // Change column name uniqueness
    HashSet<String> columnNameSet = new HashSet<>();
    for (String colName : columnNames) {
      if (!columnNameSet.add(colName)) {
        throw new ValidationPipeException("Duplicate column found: " + colName);
      }
    }
    prepareNext();
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    T toReturn = next;
    prepareNext();
    return toReturn;
  }

  /**
   * @return The ordered list of column names provided by this pipe.
   * The names are either taken from the header row, or from the column names list as provided to the constructor. Note that these names are the names
   * passed to the row converter along with the corresponding column values.
   */
  public String[] getColumnNames() {
    return columnNames;
  }
  
  private void prepareNext() throws PipeException, InterruptedException {
    String nextFromIn = input.next();
    if (nextFromIn == null) {
      next = null;
    } else {
      HashMap<String, String> row = new HashMap<>();
      String[] rowValues = splitRow(nextFromIn);
      if (rowValues.length != columnNames.length) {
        throw new ValidationPipeException("Number of columns in input is " + rowValues.length + ", but expected " + columnNames.length);
      }
      for (int i = 0; i < columnNames.length; i++) {
        row.put(columnNames[i], rowValues[i]);
      }
      
      next = rowConverter.apply(row);
    }
  }

  /**
   * @param row the row for splitting according to the delimiter. Values are trimmed.
   */
  private String[] splitRow(String row) throws PipeException {
    try {
      String[] res = csvParser.parseLine(row);
      for (int i = 0; i < res.length; i++) {
        String value = res[i];
        if (value != null) {
          res[i] = value.trim();
        }
      }
      return res;
    } catch (IOException e) {
      // We map this to a validation error, since the only possible errors in parseLine(..) above are validation related, and not IO related.
      throw new ValidationPipeException("Illegal CSV row - |" + row + "|", e);
    }
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public T peek() {
    return next;
  }

  @Override
  public float getProgress() {
    return input.getProgress();
  }
}
