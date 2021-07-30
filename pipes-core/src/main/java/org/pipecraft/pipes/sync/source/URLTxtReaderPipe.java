package org.pipecraft.pipes.sync.source;

import org.pipecraft.infra.io.FileReadOptions;
import org.pipecraft.infra.io.FileUtils;
import org.pipecraft.infra.io.SizedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.pipecraft.pipes.exceptions.HttpPipeException;
import org.pipecraft.pipes.exceptions.IOPipeException;
import org.pipecraft.pipes.exceptions.PipeException;

/**
 * A source pipe providing the lines of a text resource defined by a URL. 
 * All valid {@link URL} scheme types supported by the JDK are supported here, and not just HTTP URLs.
 * 
 * For HTTP URLs, server side exceptions are represented as {@link HttpPipeException}, which also include the status code.
 * Redirects are supported.
 *  
 * Supports decompression.
 *
 * @author Michal Rockban, Eyal Schneider
 */
public class URLTxtReaderPipe extends InputStreamPipe<String> {
  private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 2000;
  private static final int DEFAULT_READ_TIMEOUT_MS = 1000;
  
  private final Charset charset;
  private final URL url;
  private final FileReadOptions readOptions;
  private final int connectionTimeoutMs;
  private final int readTimeoutMs;

  private BufferedReader reader;
  private String next;

  /**
   * Constructor
   *
   * @param url the URL of the file.
   * @param charset The charset used
   * @param options The file read options
   * @param connectTimeoutMs Connect timeout in milli seconds
   * @param readTimeoutMs Read timeout in millis seconds
   */
  public URLTxtReaderPipe(URL url, Charset charset, FileReadOptions options, int connectTimeoutMs, int readTimeoutMs) {
    super(0, options.getCompression()); // Buffer size set to 0 in input stream because we already have a BufferedReader layer
    this.url = url;
    this.charset = charset;
    this.readOptions = options;
    this.connectionTimeoutMs = connectTimeoutMs;
    this.readTimeoutMs = readTimeoutMs;
  }

  /**
   * Constructor
   * 
   * Uses defaults: UTF8 charset, no compression, and default timeouts.
   * @param url the URL of the file.
   */
  public URLTxtReaderPipe(URL url) {
    this(url, StandardCharsets.UTF_8, new FileReadOptions(), DEFAULT_CONNECTION_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS);
  }

  @Override
  public String next() throws PipeException, InterruptedException {
    String toReturn = next;
    prepareNext();
    return toReturn;
  }

  @Override
  public String peek() {
    return next;
  }

  @Override
  protected SizedInputStream createInputStream() throws IOException, IOPipeException, InterruptedException {
    if (url.getProtocol().equals("http") || url.getProtocol().equals("https")) {
      // We prefer using the newer HttpClient for HTTP URLs and not the old URLConnection, which has some issues with how it deals with status codes.
      HttpClient client = HttpClient.newBuilder().followRedirects(Redirect.NORMAL).build();
      HttpRequest request;
      try {
        request = HttpRequest.newBuilder().uri(url.toURI()).build();
      } catch (URISyntaxException e) {
        throw new IOException("URL is not a valid URI: " + url, e);
      }

      HttpResponse<InputStream> response = client.send(request, BodyHandlers.ofInputStream());
      int statusCode = response.statusCode();
      if (statusCode != HttpURLConnection.HTTP_OK) {
        FileUtils.closeSilently(response.body());
        throw new HttpPipeException(statusCode);
      }
      Long length = response.headers().firstValue("content-length").map(Long::parseLong).orElse(null);
      return new SizedInputStream(response.body(), length);
    } else {
      URLConnection connection = url.openConnection();
      connection.setConnectTimeout(connectionTimeoutMs);
      connection.setReadTimeout(readTimeoutMs);
      return new SizedInputStream(connection.getInputStream(), connection.getContentLengthLong());
    }
  }

  @Override
  public void start() throws PipeException, InterruptedException {
    super.start();
    this.reader = toReader(getInputStream(), charset, readOptions.getBufferSize());
    prepareNext();
  }

  @Override
  public void close() throws IOException {
    FileUtils.close(reader);
  }

  private void prepareNext() throws IOPipeException {
    try {
      next = reader.readLine();
    } catch (IOException e) {
      throw new IOPipeException(e);
    }
  }

  private static BufferedReader toReader(InputStream is, Charset charset, int bufferSize) {
    return new BufferedReader(new InputStreamReader(is, charset), bufferSize);
  }
}
