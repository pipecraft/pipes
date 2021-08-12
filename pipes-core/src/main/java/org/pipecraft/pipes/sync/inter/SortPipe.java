package org.pipecraft.pipes.sync.inter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.pipecraft.pipes.sync.Pipe;
import org.pipecraft.infra.io.Compression;
import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.FileWriteOptions;
import org.pipecraft.pipes.serialization.CodecFactory;
import org.pipecraft.pipes.serialization.EncoderFactory;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.pipes.serialization.DecoderFactory;
import org.pipecraft.pipes.sync.source.BinInputReaderPipe;
import org.pipecraft.pipes.sync.source.CollectionReaderPipe;
import org.pipecraft.pipes.sync.source.EmptyPipe;
import org.pipecraft.pipes.terminal.BinFileWriterPipe;

/**
 * An intermediate pipe performing a sort on an input pipe, without deduping.
 * Uses a limit of number of items that can be handled at once in memory.
 * When the input file is larger, local disk is used for storing and reading intermediate results.
 *
 * @param <T> The items data type
 *
 * @author Eyal Schneider
 */
public class SortPipe<T> implements Pipe<T> {

  private final Pipe<T> input;
  private final File tmpFolder;
  private final Comparator<T> comparator;
  private final int maxItemsInMemory;
  private final Compression tempFilesCompression;
  private final List<File> sortedFiles;
  private final EncoderFactory<? super T> encoderFactory;
  private final DecoderFactory<T> decoderFactory;
  private volatile Pipe<T> finalOutputPipe;

  /**
   * Constructor
   *
   * @param input The input pipe
   * @param maxItemsInMemory The maximum number of items to be accumulated in memory at once.
   * @param tmpFolder The folder where to store intermediate results if needed. Temp files are deleted automatically by this pipe.
   * @param encoderFactory Specifies how to serialize items to disk (in case temp disk space is required)
   * @param decoderFactory Specifies how to deserialize items from disk (in case temp disk space is required).
   * @param comparator The comparator defining order relation on type T.
   * @param tempFilesCompression What compression should be used for temp files
   */
  public SortPipe(Pipe<T> input, int maxItemsInMemory, File tmpFolder, EncoderFactory<? super T> encoderFactory, DecoderFactory<T> decoderFactory, Comparator<T> comparator, Compression tempFilesCompression) {
    this.input = input;
    this.tmpFolder = tmpFolder;
    this.encoderFactory = encoderFactory;
    this.decoderFactory = decoderFactory;
    this.comparator = comparator;
    this.sortedFiles = new ArrayList<>();
    this.maxItemsInMemory = maxItemsInMemory;
    this.tempFilesCompression = tempFilesCompression;
  }

  /**
   * Constructor
   *
   * Uses compression on temp files.
   *
   * @param input The input pipe
   * @param maxItemsInMemory The maximum number of items to be accumulated in memory at once.
   * @param tmpFolder The folder where to store intermediate results if needed. Temp files are deleted automatically by this pipe.
   * @param encoderFactory Specifies how to serialize items to disk (in case temp disk space is required)
   * @param decoderFactory Specifies how to deserialize items from disk (in case temp disk space is required).
   * @param comparator The comparator defining order relation on type T
   */
  public SortPipe(Pipe<T> input, int maxItemsInMemory, File tmpFolder, EncoderFactory<? super T> encoderFactory, DecoderFactory<T> decoderFactory, Comparator<T> comparator) {
    this(input, maxItemsInMemory, tmpFolder, encoderFactory, decoderFactory, comparator, Compression.ZSTD);
  }

  /**
   * Constructor
   *
   * Uses compression on temp files.
   *
   * @param input The input pipe
   * @param maxItemsInMemory The maximum number of items to be accumulated in memory at once.
   * @param tmpFolder The folder where to store intermediate results if needed. Temp files are deleted automatically by this pipe.
   * @param codecFactory Specifies how to serialize/deserialize items to/from disk (in case temp disk space is required).
   * @param comparator The comparator defining order relation on type T
   */
  public SortPipe(Pipe<T> input, int maxItemsInMemory, File tmpFolder, CodecFactory<T> codecFactory, Comparator<T> comparator) {
    this(input, maxItemsInMemory, tmpFolder, codecFactory, codecFactory, comparator, Compression.ZSTD);
  }

  /**
   * Constructor
   *
   * Uses compression on temp files, and uses system default temp folder
   *
   * @param input The input pipe
   * @param maxItemsInMemory The maximum number of items to be accumulated in memory at once.
   * @param codecFactory Specifies how to serialize/deserialize items to/from disk (in case temp disk space is required).
   * @param comparator The comparator defining order relation on type T
   */
  public SortPipe(Pipe<T> input, int maxItemsInMemory, CodecFactory<T> codecFactory, Comparator<T> comparator) {
    this(input, maxItemsInMemory, FileUtils.getSystemDefaultTmpFolder(), codecFactory, codecFactory, comparator, Compression.ZSTD);
  }

  /**
   * Constructor
   *
   * Use this constructor for disabling disk usage and sorting in-memory only.
   * To be used with care only when the input pipe's data is expected to fit entirely in memory.
   *
   * @param input The input pipe
   * @param comparator The comparator defining order relation on type T
   */
  public SortPipe(Pipe<T> input, Comparator<T> comparator) {
    this(input, Integer.MAX_VALUE, null, null, null, comparator, Compression.ZSTD);
  }

  @Override
  public void close() throws IOException {
    FileUtils.close(input, finalOutputPipe);

    finalOutputPipe = EmptyPipe.instance(); // These line is intended to release the reference to a possibly large unused memory chunk

    for (File f : sortedFiles) {
      f.delete(); // Files are created as temp files which should be deleted on JVM shutdown anyway. Ignore if deletion fails.
    }
  }

  @Override
  public T next() throws PipeException, InterruptedException {
    return finalOutputPipe.next();
  }

  @Override
  public T peek() throws PipeException {
    return finalOutputPipe.peek();
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    input.start();

    List<T> currentChunk = new ArrayList<>(maxItemsInMemory == Integer.MAX_VALUE ? 1024 : maxItemsInMemory);
    while (true) {
      // Consume from input pipe until chunk is full or end of input is found
      while (input.peek() != null && currentChunk.size() < maxItemsInMemory) {
        currentChunk.add(input.next());
      }

      // Sort chunk
      currentChunk.sort(comparator);

      if (input.peek() == null && sortedFiles.isEmpty()) { // If we are done with a single chunk - don't use disk. Return results directly from memory.
        finalOutputPipe = new CollectionReaderPipe<>(currentChunk);
        finalOutputPipe.start();
        return;
      } else { // We are dealing with temp data in disk
        try {
          // Write current chunk
          CollectionReaderPipe<T> sortedPipe = new CollectionReaderPipe<>(currentChunk);
          File sortedFile = FileUtils.createTempFile("sort-chunk" + sortedFiles.size(), tempFilesCompression.getFileExtension(), tmpFolder);
          sortedFiles.add(sortedFile);
          try (BinFileWriterPipe<T> w = new BinFileWriterPipe<>(sortedPipe, sortedFile, new FileWriteOptions().setCompression(tempFilesCompression), encoderFactory)) {
            w.start();
          }

          if (input.peek() == null) { // End of input - build a sorted union pipeline on all intermediate files
            ArrayList<Pipe<T>> sortedPipes = new ArrayList<>();
            for (File f : sortedFiles) {
              BinInputReaderPipe<T> p = new BinInputReaderPipe<>(f, new FileReadOptions().setCompression(tempFilesCompression), decoderFactory);
              sortedPipes.add(p);
            }

            finalOutputPipe = new SortedMergePipe<>(sortedPipes, comparator);
            finalOutputPipe.start();
            return;
          } else {
            currentChunk.clear();
          }
        } catch (IOException e) {
          throw new IOPipeException(e);
        }
      }
    }
  }

  @Override
  public float getProgress() {
    return finalOutputPipe.getProgress();
  }
}
