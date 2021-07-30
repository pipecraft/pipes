package org.pipecraft.infra.io;

import static net.minidev.json.parser.JSONParser.DEFAULT_PERMISSIVE_MODE;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import org.pipecraft.infra.sets.StreamSampler;
import org.pipecraft.infra.concurrent.ParallelTaskProcessor;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

/**
 * A set of utility methods for manipulating files
 *
 * @author Eyal Schneider
 */
public class FileUtils {

  public static final String CSV_GZ_EXTENSION = ".csv.gz";
  public static final String CSV_ZSTD_EXTENSION = ".csv.zst";
  public static final String ZSTD_EXTENSION = Compression.ZSTD.getFileExtension();
  public static final String CSV_EXTENSION = ".csv";
  public static final String NDJSON_EXTENSION = ".ndjson";
  public static final String GZ_EXTENSION = Compression.GZIP.getFileExtension();
  public static final Charset UTF8 = StandardCharsets.UTF_8;

  /**
   * Utility for loading the lines of a classpath file (UTF8).
   *
   * @param filename A filename (in classpath). Must refer to a textual file in UTF8.
   * @return The list of strings, each representing a line
   */
  public static List<String> getLinesFromClasspath(String filename) throws IOException {
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
    InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
    return getListFromReader(isr);
  }

  /**
   * Utility for loading the lines of a local text file
   *
   * @param file The file to read the lines from
   * @param charset The charset the text file is encoded with
   * @param options the file reading options
   * @return The list of strings, each representing a line
   */
  public static List<String> getLinesFromFile(File file, Charset charset, FileReadOptions options) throws IOException {
    BufferedReader reader = getReader(file, charset, options);
    return getListFromReader(reader);
  }

  /**
   * Utility for loading the lines of a local text file (UTF8).
   *
   * @param file The file to read the lines from (UTF8)
   * @return The list of strings, each representing a line
   */
  public static List<String> getLinesFromFile(File file) throws IOException {
    return getLinesFromFile(file, StandardCharsets.UTF_8, new FileReadOptions());
  }

  /**
   * Utility for loading the lines of a local text file into a set
   *
   * @param file The file to read the lines from
   * @param charset The charset the text file is encoded with
   * @param options the file reading options
   * @return The set of strings, each representing a unique line value
   */
  public static Set<String> getLinesFromFileAsSet(File file, Charset charset, FileReadOptions options) throws IOException {
    BufferedReader reader = getReader(file, charset, options);
    return getSetFromReader(reader);
  }

  /**
   * Utility for loading the lines of a local text file (UTF8) into a set
   *
   * @param file The file to read the lines from (UTF8)
   * @return The set of strings, each representing a unique line value
   */
  public static Set<String> getLinesFromFileAsSet(File file) throws IOException {
    return getLinesFromFileAsSet(file, StandardCharsets.UTF_8, new FileReadOptions());
  }

  /**
   * Utility for loading the lines from a reader
   *
   * @param reader The reader to read from. No need to be a buffered reader. Closed at the end of this call, regardless of IO exceptions.
   * @return The list of strings, each representing a line
   */
  public static List<String> getListFromReader(Reader reader) throws IOException {
    BufferedReader br = new BufferedReader(reader);
    try {
      ArrayList<String> res = new ArrayList<String>();
      String line = br.readLine();
      while (line != null) {
        res.add(line);
        line = br.readLine();
      }
      return res;
    } finally {
      br.close();
    }
  }

  /**
   * Utility for loading the lines from a reader, into a deduped set
   *
   * @param reader The reader to read from. No need to be a buffered reader. Closed at the end of this call, regardless of IO exceptions.
   * @return The set of strings, each representing a line
   */
  public static Set<String> getSetFromReader(Reader reader) throws IOException {
    try (BufferedReader br = new BufferedReader(reader)){
      Set<String> res = new HashSet<>();
      String line = br.readLine();
      while (line != null) {
        res.add(line);
        line = br.readLine();
      }
      return res;
    }
  }

  /**
   * Utility for loading JSON objects from classpath files
   *
   * @param filename A filename (in classpath)
   * @return The json object as read from the given file
   * @throws IOException
   * @throws {@link ParseException}
   */
  public static JSONObject getJSONFromClasspath(String filename) throws IOException, ParseException {
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
    return getJSONFromInpuStream(is);
  }

  /**
   * Utility for loading a list of JSON objects from classpath files, assuming that every line has
   * one JSON object.
   *
   * @param filename A filename (in classpath)
   * @return The list of json objects, one per line in the file
   * @throws IOException
   * @throws {@link ParseException}
   */
  public static List<JSONObject> getJSONListFromClasspath(String filename) throws IOException,
      ParseException {
    JSONParser parser = new JSONParser(DEFAULT_PERMISSIVE_MODE);

    try (
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
        BufferedReader br = getReader(is, StandardCharsets.UTF_8, new FileReadOptions())) {
      ArrayList<JSONObject> res = new ArrayList<>();
      String line = br.readLine();
      while (line != null) {
        res.add(parser.parse(line, JSONObject.class));
        line = br.readLine();
      }
      return res;
    }
  }

  /**
   * Utility for loading string from classpath files
   *
   * @param filename A filename (in classpath)
   * @return The string as read from the given file
   * @throws IOException
   */
  public static String getStringFromClasspath(String filename) throws IOException {
    try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename)) {
      return IOUtils.toString(is, StandardCharsets.UTF_8);
    }
  }

  /**
   * @param is An input stream to read the json from. Expected to have a single valid json object.
   * The stream is closed by this method.
   * @return The json object as read from the given stream
   * @throws IOException
   * @throws {@link ParseException}
   */
  public static JSONObject getJSONFromInpuStream(InputStream is) throws IOException, ParseException {
    JSONParser parser = new JSONParser(DEFAULT_PERMISSIVE_MODE);
    try (InputStream jsonStream = is) {
      return parser.parse(jsonStream, JSONObject.class);
    }
  }

  /**
   * @param file A file to read as json. Expected to have one valid json object.
   * @return The json object as read from the given file
   * @throws IOException
   * @throws {@link ParseException}
   */
  public static JSONObject getJSONFromFile(File file) throws IOException, ParseException {
    InputStream is = new FileInputStream(file);
    return getJSONFromInpuStream(is);
  }

  /**
   * Utility for loading a list of JSON objects from a file, assuming that every line has
   * one JSON object.
   *
   * @param file The file to read from
   * @return The list of json objects, each one read from one line in the file
   * @throws IOException
   * @throws {@link ParseException}
   */
  public static List<JSONObject> getJSONListFromFile(File file) throws IOException, ParseException {
    JSONParser parser = new JSONParser(DEFAULT_PERMISSIVE_MODE);
    try (
        BufferedReader br = getReader(file)) {
      ArrayList<JSONObject> res = new ArrayList<>();
      String line = br.readLine();
      while (line != null) {
        res.add(parser.parse(line, JSONObject.class));
        line = br.readLine();
      }
      return res;
    }
  }

  /**
   * @param json The json object to write
   * @param file A file to write the json to. The file is being overridden.
   * @throws IOException
   */
  public static void writeJSONToFile(JSONObject json, File file) throws IOException {
    try (BufferedWriter w = getWriter(file)) {
      w.write(json.toJSONString());
    }
  }

  /**
   * @param jsonList The list of json objects to write, one per line
   * @param file A file to write the json objects to
   * @throws IOException
   */
  public static void writeJSONListToFile(List<JSONObject> jsonList, File file) throws IOException, ParseException {
    try (BufferedWriter w = getWriter(file)) {
      for (JSONObject json : jsonList) {
        w.write(json.toJSONString());
      }
    }

  }

  /**
   * Reads and parses a CSV file with no headers. Supports null values.
   *
   * @param file The CSV file to read from
   * @return A list of lines, each composed of a list of parsed columns
   */
  public static List<List<String>> readCSV(File file) throws IOException {
    return readCSV(file, new FileReadOptions());
  }

  /**
   * Reads and parses a CSV file with no headers. Supports null values.
   *
   * @param file The CSV file to read from
   * @param options (allows reading compressed CSVs for example)
   * @return A list of lines, each composed of a list of parsed columns
   */
  public static List<List<String>> readCSV(File file, Charset charset, FileReadOptions options) throws IOException {
    return readCSV(getReader(file, charset, options));
  }

  /**
   * Reads and parses a CSV file with no headers. Supports null values.
   *
   * @param csvReader The CSV file reader
   * @return A list of lines, each composed of a list of parsed columns
   */
  public static List<List<String>> readCSV(Reader csvReader) throws IOException {
    CSVFormat csvFormat = CSVFormat.DEFAULT.withNullString("null");

    List<List<String>> res = new ArrayList<>();
    try (
        Reader reader = csvReader;
        CSVParser parser = new CSVParser(reader, csvFormat)) {
      for (CSVRecord rec : parser) {
        List<String> row = new ArrayList<>();
        for (int i = 0; i < rec.size(); i++) {
          row.add(rec.get(i));
        }

        res.add(row);
      }
    }
    return res;
  }

  /**
   * Reads and parses a CSV file with no headers. Supports null values.
   *
   * @param file The CSV file to read from
   * @param options (allows reading compressed CSVs for example)
   * @return A list of lines, each composed of a list of parsed columns
   */
  public static List<List<String>> readCSV(File file, FileReadOptions options) throws IOException {
    return readCSV(file, StandardCharsets.UTF_8, options);
  }

  /**
   * Writes CSV file with no headers. Supports null values.
   *
   * @param lines the lines to write. The iteration order defines the line write order.
   * @param f The file to write to
   */
  public static void writeCSV(List<List<String>> lines, File f) throws IOException {
    writeCSV(lines, f, cells -> cells.toArray(new String[cells.size()]));
  }

  /**
   * Writes CSV file with no headers. Supports null values.
   * receives the toLineTransformer  and applies it on each line.
   *
   * @param lines the lines to write. The iteration order defines the line write order.
   * @param toLineTransformer transformer of each object into csv columns
   * @param f The file to write to
   * @param options The file writing options
   */
  public static <T> void writeCSV(Collection<T> lines, File f, Function<T, Object[]> toLineTransformer, FileWriteOptions options) throws IOException {
    writeCSV(lines.iterator(), f, toLineTransformer, options);
  }

  /**
   * Writes items from iterator into the given csv file. Including headers.
   *
   * @param iterator the objects to write in each row
   * @param file the file to write
   * @param toLineTransformer transformer of each object into csv columns
   * @param options file write options
   * @param headers the csv file headers. optional - if null/empty - ignored
   * @param <T> the entity type
   * @throws IOException on failure
   */
  public static <T> void writeCSV(Iterator<T> iterator, File file, Function<T, Object[]> toLineTransformer, FileWriteOptions options, String... headers) throws IOException {
    CSVFormat csvFormat = CSVFormat.DEFAULT.withNullString("null");
    try (
        BufferedWriter bw = getWriter(file, options);
        CSVPrinter p = new CSVPrinter(bw, csvFormat)) {
      //writing headers if provided
      if (headers != null && headers.length > 0) {
        p.printRecord((Object[]) headers);
      }

      //writing file
      while (iterator.hasNext()) {
        p.printRecord(toLineTransformer.apply(iterator.next()));
      }
    }
  }

  /**
   * Writes CSV file with no headers. Supports null values.
   * receives the toLineTransformer  and applies it on each line.
   *
   * @param lines the lines to write. The iteration order defines the line write order.
   * @param f The file to write to
   */
  public static <T> void writeCSV(Collection<T> lines, File f, Function<T, Object[]> toLineTransformer) throws IOException {
    writeCSV(lines, f, toLineTransformer, new FileWriteOptions());
  }

  /**
   * Writes the given lines to a file.
   *
   * @param lines the lines to write. The iteration order defines the line write order.
   * @param f The file to write to
   * @param options The file writing options
   */
  public static void writeLines(Collection<String> lines, File f, FileWriteOptions options) throws IOException {
    try (BufferedWriter bw = getWriter(f, options)) {
      for (String line : lines) {
        bw.write(line);
        bw.newLine();
      }
    }
  }

  /**
   * Writes the given lines to a file, using default file write options
   *
   * @param lines the lines to write. The iteration order defines the line write order.
   * @param f The file to write to
   */
  public static void writeLines(Collection<String> lines, File f) throws IOException {
    writeLines(lines, f, new FileWriteOptions());
  }

  /**
   * creates a folder, or keeps it if exists
   *
   * @param folderPath - path (including the name) of the folder
   * @return The new created folder
   */
  public static File createFolder(String folderPath) throws IOException {
    File folder = new File(folderPath);
    folder.mkdirs();
    if (!folder.isDirectory()) {
      throw new IOException("Folder path " + folderPath + " doesn't point to a folder.");
    }
    return folder;
  }


  /**
   * creates a folder
   *
   * @param parentFolder the parent folder that should contain the folder
   * @param folderPath path (including the name) of the folder
   * @return The new requested folder (existing or created), after verifying its existence
   */
  public static File createFolder(File parentFolder, String folderPath) throws IOException {
    File folder = new File(parentFolder, folderPath);
    folder.mkdirs();
    if (!folder.isDirectory()) {
      throw new IOException("Folder path " + folderPath + " doesn't point to a folder.");
    }
    return folder;
  }

  private static void sort(File[] filesToSort, File target, File tmpFolder, int memLimitMb) throws IOException, InterruptedException {
    String srcName = filesToSort.length > 1 ? filesToSort[0].getParentFile().getName() : filesToSort[0].getName();
    Path tmpSubFolderPath = Files.createTempDirectory(tmpFolder.toPath(), "sort_" + srcName);
    File tmpSubFolder = tmpSubFolderPath.toFile();
    tmpSubFolder.deleteOnExit();
    File errFile = null;
    Process sortProcess = null;
    try {
      List<String> args = new ArrayList<>();
      args.add("sort");
      for (File f : filesToSort) {
        args.add(f.getAbsolutePath());
      }
      args.add("--output");
      args.add(target.getAbsolutePath());
      args.add("--buffer-size");
      args.add(memLimitMb + "m");
      args.add("-T"); // Temp folder location
      args.add(tmpSubFolder.getAbsolutePath());
      args.add("-u"); // Deduping

      ProcessBuilder pb = new ProcessBuilder(args);
      pb.environment().put("LC_ALL", "C"); // To force case sensitive lexicographic ordering
      errFile = File.createTempFile("sort_err", ".log", filesToSort[0].getParentFile());
      errFile.deleteOnExit();
      pb.redirectError(errFile);

      sortProcess = pb.start();
      int exitValue = sortProcess.waitFor();
      if (exitValue != 0) {
        List<String> l = IOUtils.readLines(new FileInputStream(errFile), StandardCharsets.UTF_8);
        throw new IOException("File sorting failed with code " + exitValue + ". Description: " + StringUtils.join(l, "\n"));
      }
    } finally {
      if (sortProcess != null) {
        sortProcess.destroyForcibly();
      }
      deleteFiles(errFile, tmpSubFolder);
    }
  }

  /**
   * Serves for sorting large files on the file system, limiting the memory used in the process.
   * NOTE: this method is not cross-platform, and uses linux' sort command. Use with care.
   */
  public static void sort(File src, File trgt, File tmpDir, int memLimit)
      throws IOException, InterruptedException {
    File[] filesToSort;
    if (src.isDirectory()) {
      filesToSort = src.listFiles();
    } else {
      filesToSort = new File[]{src};
    }
    sort(filesToSort, trgt, tmpDir, memLimit);
  }

  /**
   * <p>Same as {@link this#sort(File, FilenameFilter, File, File, int)}, but enables to supply a
   * {@link FilenameFilter} to exclude files from the provided directory from being sorted.</p>
   *
   * @param srcDir The file/folder to sort. The file/s are assumed to be CSV, and lines are sorted
   * lexicographically. If a folder is used, all files inside it are sorted as into one output
   * file. Lines are being deduped.
   * @param fnameFilter a predicate for including files in the sort operation
   * @param trgt The target file to write to
   * @param tmpDir The temp folder to use for intermediate sorting results. Note that a new temp
   * subfolder is created under this folder, and finally is cleaned up.
   * @param memLimit The limit of memory usage, in megabytes
   */
  public static void sort(File srcDir, FilenameFilter fnameFilter, File trgt, File tmpDir, int memLimit)
      throws IOException, InterruptedException {
    File[] filesToSort = srcDir.listFiles(fnameFilter);
    sort(filesToSort, trgt, tmpDir, memLimit);
  }

  /**
   * @param source The file to examine
   * @param options File reading options
   * @return The number of lines in the file
   */
  public static int lineCount(File source, FileReadOptions options) throws IOException {
    try (BufferedReader br = getReader(source, options)) {
      int count = 0;
      String line = br.readLine();
      while (line != null) {
        count++;
        line = br.readLine();
      }
      return count;
    }
  }

  /**
   * Counts lines in a file, which is assumed to be UTF8, not compressed.
   *
   * @param source The file to examine
   * @return The number of lines in the file
   */
  public static int lineCount(File source) throws IOException {
    return lineCount(source, new FileReadOptions());
  }

  /**
   * @param prefixPath The path to serve as a prefix. May be empty. May end with '/'.
   * @param suffixPath A path to serve as a suffix. May be empty. May start with '/'.
   * @return The concatenated path, using '/' as path separator.
   */
  public static String joinPaths(String prefixPath, String suffixPath) {
    if (prefixPath.isEmpty()) {
      return suffixPath;
    }
    if (suffixPath.isEmpty()) {
      return prefixPath;
    }

    if (prefixPath.endsWith("/")) {
      prefixPath = prefixPath.substring(0, prefixPath.length() - 1);
    }
    if (suffixPath.startsWith("/")) {
      suffixPath = suffixPath.substring(1);
    }

    return prefixPath + "/" + suffixPath;
  }

  /**
   * Deletes a file/folder, ignoring failures. In case of a failure reports to the log.
   *
   * @param f The file to delete. May be null. May be a folder. In case of a folder, the complete folder is deleted, recursively.
   * @param logger The logger to log to in case deletion fails (as warning). Use null for no logging.
   */
  public static void deleteFile(File f, org.slf4j.Logger logger) {
    if (f != null) {
      if (f.isDirectory()) {
        try {
          org.apache.commons.io.FileUtils.deleteDirectory(f);
        } catch (IOException e) {
          if (logger != null) {
            logger.warn("Failed deleting temp folder: " + f.getAbsolutePath());
          }
        }
      } else {
        if (!f.delete() && logger != null) {
          logger.warn("Failed deleting temp file: " + f.getAbsolutePath());
        }
      }
    }
  }

  /**
   * Deletes a set of files/folders, ignoring failures.
   *
   * @param logger The logger to log to in case deletion fails (as warning). Use null for no logging.
   * @param files The files to delete. Null files are ignored. File objects may be folders - In this case, the complete folder is deleted, recursively.
   */
  public static void deleteFiles(org.slf4j.Logger logger, File... files) {
    for (File f : files) {
      deleteFile(f, logger);
    }
  }

  /**
   * Deletes a set of files/folders, ignoring failures.
   *
   * @param files The files to delete. Null files are ignored. File objects may be folders - In this case, the complete folder is deleted, recursively.
   */
  public static void deleteFiles(File... files) {
    deleteFiles(null, files);
  }

  /**
   * Uses a streaming sampling algorithm to select M lines from a given file, guaranteeing equal
   * probability for all subsets of the requested sample size (See https://eyalsch.wordpress.com/2010/04/01/random-sample/ for details).
   *
   * Time complexity: Linear in size of the input file
   * Space complexity: O(1)
   *
   * @param srcFile The file to sample lines from. Assumed to be textual, UTF8.
   * @param srcSize The number of lines in srcFile.
   * @param dstFile The destination file to contain the produced sample
   * @param toSample The number of lines to sample.
   * @throws IOException In case of read/write error
   */
  public static void sampleLines(File srcFile, int srcSize, File dstFile, int toSample) throws IOException {
    Random rnd = new Random();
    StreamSampler sampler = new StreamSampler(rnd, srcSize, toSample);
    try (
        BufferedReader r = getReader(srcFile);
        BufferedWriter w = getWriter(dstFile);
    ) {
      for (int i = 0; i < srcSize; i++) {
        String line = r.readLine();
        if (sampler.accept()) {
          w.write(line);
          w.newLine();
        }
      }
    }
  }

  /**
   * Saves the head of a given file in another new file/
   *
   * @param file The input text file
   * @param lineCountToCopy The number of lines to copy from the input file. Less lines may be copied if the file line count is lower than this number.
   * @param outputFile The output file
   */
  public static void saveFileHead(File file, int lineCountToCopy, File outputFile) throws IOException {
    try (
        FileInputStream fis = new FileInputStream(file);
        InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
        BufferedReader br = new BufferedReader(isr);
        FileOutputStream fos = new FileOutputStream(outputFile);
        OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
        BufferedWriter bw = new BufferedWriter(osw);
    ) {
      String line = br.readLine();
      int copiedLines = 0;
      while (line != null && copiedLines < lineCountToCopy) {
        bw.write(line);
        copiedLines++;
        bw.newLine();
        line = br.readLine();
      }
    }
  }

  /**
   * Compresses a given file (to gz), leaving the original
   *
   * @param inFile The file to compress
   * @return The new compressed file, created in the same folder, with the same name as the original, but with the ".gz" suffix
   * @throws IOException If reading/writing fails
   */
  public static File gzip(File inFile) throws IOException {
    return compress(inFile, Compression.GZIP);
  }

  /**
   * Compresses a given file, leaving the original.
   * Uses the default compression level of the chosen compression.
   *
   * @param inFile The file to compress
   * @param compression The compression to use. Using Compression.NONE does nothing and returns the input file.
   * @return The new compressed file, created in the same folder, with the same name as the original, but with the suffix corresponding to the selected
   * compression.
   * @throws IOException If reading/writing fails
   */
  public static File compress(File inFile, Compression compression) throws IOException {
    return compress(inFile, compression, compression.getDefaultCompressionLevel());
  }

  /**
   * Compresses a given file, leaving the original
   *
   * @param inFile The file to compress
   * @param compression The compression to use. Using Compression.NONE does nothing and returns the input file.
   * @param compressionLevel The compression level to use. Specific to the chosen compression scheme and should be used with caution.
   * @return The new compressed file, created in the same folder, with the same name as the original, but with the suffix corresponding to the selected
   * compression.
   * @throws IOException If reading/writing fails
   */
  public static File compress(File inFile, Compression compression, int compressionLevel) throws IOException {
    if (compression == Compression.NONE) {
      return inFile;
    }

    byte[] buffer = new byte[8192];
    File outFile = new File(inFile.getParentFile(), inFile.getName() + compression.getFileExtension());

    try (InputStream bis = getInputStream(inFile, new FileReadOptions());
        OutputStream os = new FileOutputStream(outFile);
        OutputStream cos = getCompressionOutputStream(os, compression, compressionLevel);
    ) {
      int count = bis.read(buffer);
      while (count != -1) {
        cos.write(buffer, 0, count);
        count = bis.read(buffer);
      }
    }
    return outFile;
  }

  /**
   * Compresses all files in the given folder, deleting the originals
   * Original file names are appended the corresponding compression suffix (.gz/.zst).
   *
   * @param folder The input folder
   * @param compression The compression to use. NONE is illegal.
   * @param parallelism The number of threads to use
   * @throws IOException
   */
  public static void compressAll(File folder, Compression compression, int parallelism) throws IOException {
    compressAll(folder, compression, compression.getDefaultCompressionLevel(), parallelism);
  }

  /**
   * Compresses all files in the given folder, deleting the originals
   * Original file names are appended the corresponding compression suffix (.gz/.zst).
   *
   * @param folder The input folder
   * @param compression The compression to use. NONE is illegal.
   * @param compressionLevel The compression level to use. Specific to the chosen compression scheme and should be used with caution.
   * @param parallelism The number of threads to use
   * @throws IOException in case of IO error or if the thread is interrupted
   */
  public static void compressAll(File folder, Compression compression, int compressionLevel, int parallelism) throws IOException {
    if (compression == Compression.NONE) {
      throw new IllegalArgumentException("Must specify a valid compression");
    }
    try {
      ParallelTaskProcessor.runFailable(Arrays.asList(folder.listFiles()), parallelism, f -> {
        compress(f, compression, compressionLevel);
        deleteFiles(f);
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  /**
   * Decompresses a given file (from gz), leaving the original
   *
   * @param inFile The file to decompress
   * @return The new decompressed file, in the same folder as the input file. The name will be the same as the original, with the ".gz" suffix removed.
   * In case of no such suffix of the original file, the suffix ".decompressed" is added to the target file
   * @throws IOException If reading/writing fails
   */
  public static File gunzip(File inFile) throws IOException {
    return decompress(inFile, Compression.GZIP);
  }

  /**
   * Decompresses a given file, leaving the original
   *
   * @param inFile The file to decompress
   * @param compression The compression to assume
   * @return The new decompressed file, in the same folder as the input file. The name will be the same as the original, with the corresponding suffix removed (.gz, or .zst)
   * In case of no such suffix of the original file, the suffix ".decompressed" is added to the target file.
   * @throws IOException If reading/writing fails
   */
  public static File decompress(File inFile, Compression compression) throws IOException {
    byte[] buffer = new byte[8192];

    String targetName = inFile.getName();
    targetName = targetName.endsWith(compression.getFileExtension()) ?
        targetName.substring(0, targetName.length() - compression.getFileExtension().length()) : (targetName + ".decompressed");

    File outFile = new File(inFile.getParentFile(), targetName);

    try (InputStream is = getInputStream(inFile, new FileReadOptions().setCompression(compression));
        OutputStream os = getOutputStream(outFile, new FileWriteOptions())) {

      int count = is.read(buffer);
      while (count != -1) {
        os.write(buffer, 0, count);
        count = is.read(buffer);
      }
    }
    return outFile;
  }

  /**
   * Decompresses all files in the given folder, deleting the originals
   * File names are removed the relevant extension (.gz/.zst), and if not found, the name is appended with ".decompressed".
   *
   * @param folder The input folder
   * @param compression The compression to assume. NONE is illegal.
   * @param parallelism The number of threads to use
   * @throws IOException in case of IO error or if the thread is interrupted
   */
  public static void decompressAll(File folder, Compression compression, int parallelism) throws IOException {
    if (compression == Compression.NONE) {
      throw new IllegalArgumentException("Must specify a valid compression");
    }
    try {
      ParallelTaskProcessor.runFailable(Arrays.asList(folder.listFiles()), parallelism, f -> {
        decompress(f, compression);
        deleteFiles(f);
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  /**
   * Compares two given files
   *
   * @param file1 File #1
   * @param file2 File #2
   * @return True if and only if the byte sequences of the two files are identical
   * @throws IOException
   */
  public static boolean areIdentical(File file1, File file2) throws IOException {
    byte[] buffer1 = new byte[8096];
    byte[] buffer2 = new byte[8096];
    try (
        InputStream is1 = new FileInputStream(file1);
        InputStream is2 = new FileInputStream(file2);
    ) {
      int count1 = IOUtils.read(is1, buffer1, 0, 8096);
      int count2 = IOUtils.read(is2, buffer2, 0, 8096);
      while (count1 != 0 && count2 != 0) {
        if (!Arrays.equals(buffer1, buffer2)) {
          return false;
        }
        count1 = IOUtils.read(is1, buffer1, 0, 8096);
        count2 = IOUtils.read(is2, buffer2, 0, 8096);
      }
      return count1 == 0 && count2 == 0;
    }
  }

  /**
   * Creates a unique temp file in the given folder. The file is deleted automatically when the JVM exits.
   *
   * @param prefix The name prefix of the temp file. Must be at least 3 char long.
   * @param suffix The name suffix of the temp file (e.g. ".csv").
   * @param folder The folder where to create the temp file, or null for default temp folder.
   * @return A new temp file in the given folder. The file is set to be deleted at JVM shutdown, but it's recommended
   * to delete it explicitly when not used anymore.
   */
  public static File createTempFile(String prefix, String suffix, File folder) throws IOException {
    File f = File.createTempFile(prefix, suffix, folder);
    f.deleteOnExit();
    return f;
  }

  /**
   * Creates a unique temp file in the system's default tmp folder. The file is deleted automatically when the JVM exits.
   *
   * @param prefix The name prefix of the temp file. Must be at least 3 char long. Must not include path separators.
   * @param prefix The name suffix of the temp file (e.g. ".csv").
   * @return A new temp file in the system's temp folder. The file is set to be deleted at JVM shutdown, but it's recommended
   * to delete it explicitly when not used anymore.
   */
  public static File createTempFile(String prefix, String suffix) throws IOException {
    return createTempFile(prefix, suffix, null);
  }

  /**
   * Creates a unique file in the given folder. The file is not deleted automatically when the JVM exits.
   *
   * @param prefix The name prefix of the temp file. Must be at least 3 char long. Must not include path separators.
   * @param suffix The name suffix of the temp file (e.g. ".csv").
   * @param folder The folder where to create the temp file
   * @return A new file in the given folder
   */
  public static File createUniqueFile(String prefix, String suffix, File folder) throws IOException {
    return File.createTempFile(prefix, suffix, folder);
  }

  /**
   * Creates a unique file in the system's default tmp folder. The file is not deleted automatically when the JVM exits.
   *
   * @param prefix The name prefix of the temp file. Must be at least 3 char long. Must not include path separators.
   * @param prefix The name suffix of the temp file (e.g. ".csv").
   * @return A new file in the system's temp folder
   */
  public static File createUniqueFile(String prefix, String suffix) throws IOException {
    return File.createTempFile(prefix, suffix);
  }

  /**
   * creates a temp folder under a given parent folder
   *
   * @param prefix The name prefix for the temp folder
   * @param parentFolder The parent folder under which to create the temp folder
   * @return A new temp folder under the given parent folder. The folder is set to be deleted at JVM shutdown
   */
  public static File createTempFolder(String prefix, File parentFolder) throws IOException {
    File res = createUniqueFolder(prefix, parentFolder);
    res.deleteOnExit();
    return res;
  }

  /**
   * @return the tmp folder on the local machine
   */
  public static File getSystemDefaultTmpFolder() {
    return new File(System.getProperty("java.io.tmpdir"));
  }

  /**
   * creates a unique folder under a given parent folder
   *
   * @param prefix The name prefix for the folder
   * @param parentFolder The parent folder under which to create the folder
   * @return A new folder with unique name under the given parent folder. The folder is not set to be deleted at JVM shutdown
   */
  public static File createUniqueFolder(String prefix, File parentFolder) throws IOException {
    Path path = Files.createTempDirectory(parentFolder.toPath(), prefix);
    File res = path.toFile();
    return res;
  }

  /**
   * creates a temp folder under the system's default tmp folder
   *
   * @param prefix The name prefix for the temp folder
   * @return A new temp folder under the system's tmp folder. The folder is set to be deleted at JVM shutdown, provided that
   * it's empty or all files in it are also created as temporary.
   */
  public static File createTempFolder(String prefix) throws IOException {
    File res = createUniqueFolder(prefix);
    res.deleteOnExit();
    return res;
  }

  /**
   * creates a unique folder under the system's default tmp folder
   *
   * @param prefix The name prefix for the folder
   * @return A new folder under the system's tmp folder. The folder is not set to be deleted at JVM shutdown
   */
  public static File createUniqueFolder(String prefix) throws IOException {
    Path path = Files.createTempDirectory(prefix);
    File res = path.toFile();
    return res;
  }

  /**
   * Wraps an output stream, adding it options by the given {@link FileWriteOptions}.
   *
   * @param os The stream to wrap
   * @param options The write options
   * @return The resulting output stream (buffered)
   */
  public static BufferedOutputStream getOutputStream(OutputStream os, FileWriteOptions options) throws IOException {
    os = getCompressionOutputStream(os, options.getCompression(), options.getCompressionLevel());
    return new BufferedOutputStream(os, options.getBufferSize());
  }

  /**
   * Creates an output stream that writes into a file, using the given {@link FileWriteOptions}.
   *
   * @param f The file to write to
   * @param options The write options
   * @return The resulting output stream (buffered)
   */
  public static BufferedOutputStream getOutputStream(File f, FileWriteOptions options) throws IOException {
    if (options.isTemp()) {
      f.deleteOnExit();
    }
    OutputStream os = new FileOutputStream(f, options.isAppend());
    return getOutputStream(os, options);
  }

  /**
   * Wraps an input stream, adding the features defined by the given {@link FileReadOptions}.
   *
   * @param is The input stream to wrap
   * @param options The read options
   * @return The resulting input stream (Buffered)
   */
  public static BufferedInputStream getInputStream(InputStream is, FileReadOptions options) throws IOException {
    is = getCompressionInputStream(is, options.getCompression());
    return new BufferedInputStream(is, options.getBufferSize());
  }

  /**
   * Creates an input stream that reads from a file, using the given {@link FileReadOptions}.
   *
   * @param f The file to read from
   * @param options The read options
   * @return The resulting input stream (Buffered)
   */
  public static BufferedInputStream getInputStream(File f, FileReadOptions options) throws IOException {
    InputStream is = new FileInputStream(f);
    return getInputStream(is, options);
  }

  /**
   * Wraps a gives output stream, setting the requires write options
   * It's recommended to call this method from within a try-with-resources block.
   *
   * @param os The output stream to wrap
   * @param charset The character encoding to use
   * @param options The write options
   * @return The new buffered writer. The caller should make sure to close it.
   */
  public static BufferedWriter getWriter(OutputStream os, Charset charset, FileWriteOptions options) throws IOException {
    os = getCompressionOutputStream(os, options.getCompression(), options.getCompressionLevel());
    OutputStreamWriter osw = new OutputStreamWriter(os, charset);
    return new BufferedWriter(osw, options.getBufferSize());
  }

  /**
   * Creates a new buffered writer for a given file.
   * It's recommended to call this method from within a try-with-resources block.
   *
   * @param f The file
   * @param charset The character encoding to use
   * @param options The file writing options
   * @return The new buffered writer. The caller should make sure to close it.
   */
  public static BufferedWriter getWriter(File f, Charset charset, FileWriteOptions options) throws IOException {
    if (options.isTemp()) {
      f.deleteOnExit();
    }

    OutputStream os = new FileOutputStream(f, options.isAppend());
    return getWriter(os, charset, options);
  }

  /**
   * Creates a new buffered writer for a given file, using UTF8.
   * It's recommended to call this method from within a try-with-resources block.
   *
   * @param f The file
   * @param options The file writing options
   * @return The new buffered writer. The caller should make sure to close it.
   */
  public static BufferedWriter getWriter(File f, FileWriteOptions options) throws IOException {
    return getWriter(f, StandardCharsets.UTF_8, options);
  }

  /**
   * Creates a new buffered writer for a given file, using default file writing settings (See {@link FileWriteOptions}).
   * It's recommended to call this method from within a try-with-resources block.
   *
   * @param f The file
   * @param charset The character encoding to use
   * @return The new buffered writer. The caller should make sure to close it.
   */
  public static BufferedWriter getWriter(File f, Charset charset) throws IOException {
    return getWriter(f, charset, new FileWriteOptions());
  }

  /**
   * Creates a new buffered writer for a given file, using UTF8 and default file writing settings (See {@link FileWriteOptions}).
   * It's recommended to call this method from within a try-with-resources block.
   *
   * @param f The file
   * @return The new buffered writer. The caller should make sure to close it.
   */
  public static BufferedWriter getWriter(File f) throws IOException {
    return getWriter(f, StandardCharsets.UTF_8, new FileWriteOptions());
  }

  /**
   * @param is The input stream to wrap
   * @param compression Compression of the data supplied by the given input stream
   * @return The decompressing input stream
   */
  public static InputStream getCompressionInputStream(InputStream is, Compression compression) throws IOException {
    switch (compression) {
      case GZIP:
        return new GZIPInputStream(is);
      case ZSTD:
        return new ZstdInputStream(is);
      case NONE:
        return is;
      case LZ4:
        throw new IllegalArgumentException("Compression type LZ4 not supported yet");
    }
    throw new IllegalArgumentException("Compression type is unknown: " + compression);
  }

  /**
   * @param os The output stream to wrap
   * @param compression The required compression to apply
   * @return The compressing output stream
   */
  public static OutputStream getCompressionOutputStream(OutputStream os, Compression compression) throws IOException {
    return getCompressionOutputStream(os, compression, compression.getDefaultCompressionLevel());
  }

  /**
   * @param os Output stream
   * @param compression The required compression to apply
   * @param compressionLevel The compression level to use. Specific to the chosen compression scheme.
   * @return The compressing output stream
   */
  public static OutputStream getCompressionOutputStream(OutputStream os, Compression compression, int compressionLevel) throws IOException {
    switch (compression) {
      case GZIP:
        return new GZIPOutputStream(os) {{ def.setLevel(compressionLevel); }}; // Using anonymous inner class since GZIPOutputStream doesn't expose the deflater!
      case ZSTD:
        return new ZstdOutputStream(os, compressionLevel);
      case NONE:
        return os;
      case LZ4:
        throw new IllegalArgumentException("Compression type LZ4 not supported yet");
    }
    throw new IllegalArgumentException("Compression type is unknown: " + compression);
  }

  /**
   * Wraps an input stream and converts it to a reader.
   * It's recommended to call this method from within a try-with-resources block.
   *
   * @param is The input stream
   * @param charset The character encoding to use
   * @param options The read options
   * @return The new buffered reader. The caller should make sure to close it.
   */
  public static BufferedReader getReader(InputStream is, Charset charset, FileReadOptions options) throws IOException {
    is = getCompressionInputStream(is, options.getCompression());
    InputStreamReader isr = new InputStreamReader(is, charset);
    return new BufferedReader(isr, options.getBufferSize());
  }

  /**
   * Wraps an input stream and converts it to a reader.
   * It's recommended to call this method from within a try-with-resources block.
   * Assumes UTF8, no compression and default buffer size.
   *
   * @param is The input stream
   * @return The new buffered reader. The caller should make sure to close it.
   */
  public static BufferedReader getReader(InputStream is) throws IOException {
    return getReader(is, StandardCharsets.UTF_8, new FileReadOptions());
  }

  /**
   * Creates a new buffered reader from a given file.
   * It's recommended to call this method from within a try-with-resources block.
   *
   * @param f The file
   * @param charset The character encoding to use
   * @param options The file read options
   * @return The new buffered reader. The caller should make sure to close it.
   */
  public static BufferedReader getReader(File f, Charset charset, FileReadOptions options) throws IOException {
    InputStream is = new FileInputStream(f);
    return getReader(is, charset, options);
  }

  /**
   * Creates a new buffered reader from a given file, using UTF8.
   * It's recommended to call this method from within a try-with-resources block.
   *
   * @param f The file
   * @param options The file read options
   * @return The new buffered reader. The caller should make sure to close it.
   */
  public static BufferedReader getReader(File f, FileReadOptions options) throws IOException {
    return getReader(f, StandardCharsets.UTF_8, options);
  }

  /**
   * Creates a new buffered reader from a given file.
   * It's recommended to call this method from within a try-with-resources block.
   *
   * @param f The file
   * @param charset The character encoding to use
   * @return The new buffered reader. The caller should make sure to close it.
   */
  public static BufferedReader getReader(File f, Charset charset) throws IOException {
    return getReader(f, charset, new FileReadOptions());
  }

  /**
   * Creates a new buffered reader from a given file, using UTF8.
   * It's recommended to call this method from within a try-with-resources block.
   *
   * @param f The file
   * @return The new buffered reader. The caller should make sure to close it.
   */
  public static BufferedReader getReader(File f) throws IOException {
    return getReader(f, StandardCharsets.UTF_8, new FileReadOptions());
  }

  /**
   * Return the input stream of the given resource path
   *
   * @param path the resource path
   * @return resource's input stream
   */
  public static InputStream getInputStreamFromClasspath(String path) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    return loader.getResourceAsStream(path);
  }

  /**
   * Creates a new buffered reader from a given file.
   * It's recommended to call this method from within a try-with-resources block.
   *
   * @param path The path in classpath
   * @param charset The character encoding to use
   * @param options The file read options
   * @return The new buffered reader. The caller should make sure to close it.
   */
  public static BufferedReader getReaderFromClasspath(String path, Charset charset, FileReadOptions options) throws IOException {
    InputStream is = getInputStreamFromClasspath(path);
    is = getCompressionInputStream(is, options.getCompression());
    InputStreamReader isr = new InputStreamReader(is, charset);
    return new BufferedReader(isr, options.getBufferSize());
  }

  /**
   * Creates a new buffered reader from a given file, using UTF8.
   * It's recommended to call this method from within a try-with-resources block.
   *
   * @param path The path in classpath
   * @param options The file read options
   * @return The new buffered reader. The caller should make sure to close it.
   */
  public static BufferedReader getReaderFromClasspath(String path, FileReadOptions options) throws IOException {
    return getReaderFromClasspath(path, StandardCharsets.UTF_8, options);
  }

  /**
   * Creates a new buffered reader from a given file.
   * It's recommended to call this method from within a try-with-resources block.
   *
   * @param path The path in classpath
   * @param charset The character encoding to use
   * @return The new buffered reader. The caller should make sure to close it.
   */
  public static BufferedReader getReaderFromClasspath(String path, Charset charset) throws IOException {
    return getReaderFromClasspath(path, charset, new FileReadOptions());
  }

  /**
   * Creates a new buffered reader from a given file, using UTF8.
   * It's recommended to call this method from within a try-with-resources block.
   *
   * @param path The path in classpath
   * @return The new buffered reader. The caller should make sure to close it.
   */
  public static BufferedReader getReaderFromClasspath(String path) throws IOException {
    return getReaderFromClasspath(path, StandardCharsets.UTF_8, new FileReadOptions());
  }

  /**
   * throws exception if folder is invalid or empty.
   *
   * @throws IOException if folder is empty
   */
  public static void validateFolderNotEmpty(File folder) throws IOException {
    File[] files = folder.listFiles();
    if (files == null || files.length == 0) {
      throw new IOException("Folder " + folder.getName() + " is empty");
    }
  }

  /**
   * @param name A file name
   * @return The file extension. The extension is defined here as the suffix starting with the first '.'. If none, null is returned.
   */
  public static String getExtension(String name) {
    int ind = name.indexOf('.');
    if (ind == -1) {
      return null;
    }
    return name.substring(ind);
  }

  /**
   * @param name A file name
   * @return The file name, with the extension removed, if found. The extension is defined as the suffix starting with the first '.'.
   */
  public static String removeExtension(String name) {
    int ind = name.indexOf('.');
    if (ind != -1) {
      name = name.substring(0, ind);
    }
    return name;
  }

  /**
   * @param name A file name
   * @return The file extension. The extension is defined here as the suffix starting with the last '.'. If none, null is returned.
   */
  public static String getDotlessExtension(String name) {
    int ind = name.lastIndexOf('.');
    if (ind == -1) {
      return null;
    }
    return name.substring(ind);
  }

  /**
   * @param name A file name
   * @return The file name, with the extension removed, if found. The extension is defined here as the suffix starting with the last '.'.
   */
  public static String removeDotlessExtension(String name) {
    int ind = name.lastIndexOf('.');
    if (ind != -1) {
      name = name.substring(0, ind);
    }
    return name;
  }

  /**
   * @param fileName A file name. May include path.
   * @return The same name, but with the CSV extension removed (either .csv or .csv.gz or .csv.zst)
   */
  public static String removeCSVExtension(String fileName) {
    if (fileName.endsWith(CSV_GZ_EXTENSION)) {
      fileName = truncateEnd(fileName, CSV_GZ_EXTENSION.length());
    } else if (fileName.endsWith(CSV_EXTENSION)) {
      fileName = truncateEnd(fileName, CSV_EXTENSION.length());
    } else if (fileName.endsWith(ZSTD_EXTENSION)) {
      fileName = truncateEnd(fileName, ZSTD_EXTENSION.length());
    }

    return fileName;
  }

  /**
   * Copies a file from one location to the other
   *
   * @param src The file to copy
   * @param dst The destination file
   */
  public static void copyFile(File src, File dst) throws IOException {
    org.apache.commons.io.FileUtils.copyFile(src, dst);
  }

  /**
   * Copies a file from one location to the other, keeping the filename
   *
   * @param src The file to copy
   * @param dstFolder The destination folder
   */
  public static void copyFileToFolder(File src, File dstFolder) throws IOException {
    org.apache.commons.io.FileUtils.copyFile(src, new File(dstFolder, src.getName()));
  }

  /**
   * @param folder A local folder
   * @return The total size, in bytes, of all descendant files under the given folder
   */
  public static long getTotalSizeRecursive(File folder) {
    long total = 0;
    for (File f : folder.listFiles()) {
      if (f.isFile()) {
        total += f.length();
      } else if (f.isDirectory()) {
        total += getTotalSizeRecursive(f);
      }
    }
    return total;
  }

  private static String truncateEnd(String source, int charsToRemove) {
    return source.substring(0, source.length() - charsToRemove);
  }

  /**
   * Closes the resources in the order they are provided. Tries to close as much as possible closeables and then throws exception if needs to.
   * Any of the closeables may be null
   *
   * @param closeables the closeables that needs to be closed. Ignores null closeables inside the array.
   * @throws IOException the IOException that aggregates all the closable IOExceptions
   */
  public static void close(Closeable... closeables) throws IOException {
    close(Arrays.asList(closeables));
  }

  /**
   * Closes the resources in the order they are provided. Tries to close as much as possible closeables and then throws exception if needs to.
   * Any of the closeables may be null
   *
   * @param closeables the closeables that needs to be closed. Ignores null closeables inside the array.
   * @throws IOException the IOException that aggregates all the closable IOExceptions
   */
  public static void close(Collection<? extends Closeable> closeables) throws IOException {
    List<IOException> errors = new ArrayList<>();

    for (Closeable closeable : closeables) {
      try {
        close(closeable);
      } catch (IOException e) {
        errors.add(e);
      }
    }

    if (!errors.isEmpty()) {
      final IOException exception = new IOException("Could not bulk close all closeables");
      errors.forEach(exception::addSuppressed);
      throw exception;
    }
  }

  /**
   * Closes the closeable.
   *
   * @param closeable the closeable that needs to be closed. May be null.
   * @throws IOException if an I/O error occurs
   */
  public static void close(Closeable closeable) throws IOException {
    if (closeable != null) {
      closeable.close();
    }
  }

  /**
   * Closes the given closeables, in a silent manner (ignoring IOExceptions)
   *
   * @param closeables the closeables that needs to be closed. Any of them may be null.
   */
  public static void closeSilently(Closeable ... closeables) {
    closeSilently(Arrays.asList(closeables));
  }

  /**
   * Closes the given closeables, in a silent manner (ignoring IOExceptions)
   *
   * @param closeables the closeables that needs to be closed. Any of them may be null.
   */
  public static void closeSilently(Collection<? extends Closeable> closeables) {
    for (Closeable c : closeables) {
      try {
        close(c);
      } catch (IOException e) {
        // Ignored
      }
    }
  }

}
