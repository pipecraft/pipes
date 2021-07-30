package org.pipecraft.pipes.sync.source;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.pipes.sync.inter.CompoundPipe;
import org.pipecraft.pipes.sync.inter.ConcatPipe;
import org.pipecraft.pipes.utils.PipeSupplier;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * Reads data from multiple files in local disk under some folder, as if they were concatenated using some predefined order.
 * Files which have the gz extension are being unzipped.
 *
 * For more features and for general binary files use {@link MultiFileReaderPipe}.
 *
 * @author Eyal Schneider
 */
public class MultiTxtFileReaderPipe extends CompoundPipe<String> {
  private final File folder;
  private final Charset charset;
  private final int bufferSize;
  private final String fileRegex;
  private final Comparator<File> comparator;

  /**
   * Constructor
   *
   * @param folder The local path of the folder to read from
   * @param charset The charset used
   * @param bufferSize The read buffer to use for every file. Use 0 for default buffer size (8k)
   * @param fileRegex Used for determining which files to read from based on the file name (excluding path)
   * @param comparator A comparator on file used for defining the order at which file are read
   */
  public MultiTxtFileReaderPipe(File folder, Charset charset, int bufferSize, String fileRegex, Comparator<File> comparator) {
    this.folder = folder;
    this.charset = charset;
    this.bufferSize = bufferSize;
    this.fileRegex = fileRegex;
    this.comparator = comparator;
  }

  /**
   * Constructor
   *
   * Assumes UTF8, no file filtering and default buffer size
   *
   * @param folder The folder to read from
   * @param comparator A comparator used for defining the order at which file are read
   */
  public MultiTxtFileReaderPipe(File folder, Comparator<File> comparator) {
    this(folder, StandardCharsets.UTF_8, 0, ".*", comparator);
  }

  /**
   * Constructor
   *
   * Assumes UTF8, no file filtering, default buffer size and lexicographic order of files
   *
   * @param folder The folder to read from
   */
  public MultiTxtFileReaderPipe(File folder) {
    this(folder, StandardCharsets.UTF_8, 0, ".*", Comparator.comparing(File::getName));
  }

  @Override
  protected Pipe<String> createPipeline() throws PipeException, InterruptedException {
    List<PipeSupplier<String>> pipeGenList = getPipeGenList();
    return new ConcatPipe<>(pipeGenList);
  }

  private List<PipeSupplier<String>> getPipeGenList() {
    File[] files = folder.listFiles(f -> f.getName().matches(fileRegex));
    Arrays.sort(files, comparator);
    List<PipeSupplier<String>> res = new ArrayList<>();
    for (File f : files) {
      FileReadOptions options = new FileReadOptions().detectCompression(f.getName());
      if (bufferSize > 0) {
        options.buffer(bufferSize);
      }
      res.add(() -> new TxtFileReaderPipe(f, charset, options));
    }

    return res;
  }
}
