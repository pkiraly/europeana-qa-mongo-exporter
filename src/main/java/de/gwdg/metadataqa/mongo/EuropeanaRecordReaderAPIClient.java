package de.gwdg.metadataqa.mongo;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.logging.Logger;

public class EuropeanaRecordReaderAPIClient implements Serializable {

  static final Logger logger = Logger.getLogger(
    EuropeanaRecordReaderAPIClient.class.getCanonicalName());

  private static final String GET_RECORD_URI =
          "http://%s/europeana-qa/record/%s.json?dataSource=mongo&batchMode=true";
  private static final String RESOLVE_FRAGMENT_URI = "http://%s/europeana-qa/resolve-json-fragment";
  private static final String RESOLVE_FRAGMENT_PARAMETERS =
          "batchMode=true&recordId=%s&jsonFragment=%s&withFieldRename=false";

  private final String USER_AGENT = "Custom Java application";
  private String host;
  private URL fragmentPostUrl;

  public EuropeanaRecordReaderAPIClient(String host) throws MalformedURLException {
    this.host = host;
    fragmentPostUrl = new URL(getFragmentUrl());
    logger.info("fragmentPostUrl: " + fragmentPostUrl.toString());
  }

  public String getRecord(String recordId) throws Exception {

    String url = getRecordUrl(recordId);

    HttpClient client = new DefaultHttpClient();
    HttpGet request = new HttpGet(url);
    request.addHeader("User-Agent", USER_AGENT);

    HttpResponse response = client.execute(request);
    //System.out.println("Response Code : " + response.getStatusLine().getStatusCode());

    InputStream in = new BufferedInputStream(response.getEntity().getContent());
    String record = readStream(in);

    return record;
  }

  private String getRecordUrl(String recordId) {
    return String.format(GET_RECORD_URI, host, recordId);
  }

  private String getFragmentUrl() {
    return String.format(RESOLVE_FRAGMENT_URI, host);
  }

  private String getFragmentParameters(String jsonFragment, String recordId) {
    String params = "";
    try {
      params = String.format(
        RESOLVE_FRAGMENT_PARAMETERS,
        recordId,
        URLEncoder.encode(jsonFragment, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return params;
  }

  public String getRecord2(String recordId) {
    URL url = null;
    HttpURLConnection urlConnection = null;
    String record = null;
    try {
      url = new URL(getRecordUrl(recordId));
      urlConnection = (HttpURLConnection) url.openConnection();
      InputStream in = new BufferedInputStream(urlConnection.getInputStream());
      record = readStream(in);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (urlConnection != null)
        urlConnection.disconnect();
    }
    return record;
  }

  public String resolveFragment(String jsonFragment, String recordId) {
    URL url = null;
    HttpURLConnection urlConnection = null;
    String record = null;
    try {
      url = new URL(getFragmentUrl() + "?" + getFragmentParameters(jsonFragment, recordId));
      urlConnection = (HttpURLConnection) url.openConnection();
      InputStream in = new BufferedInputStream(urlConnection.getInputStream());
      record = readStream(in);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (urlConnection != null)
        urlConnection.disconnect();
    }
    return record;
  }

  // HTTP POST request
  public String resolveFragmentWithPost(String jsonFragment, String recordId)
      throws Exception {
    HttpURLConnection urlConnection = (HttpURLConnection) fragmentPostUrl.openConnection();

    //add reuqest header
    urlConnection.setRequestMethod("POST");
    urlConnection.setRequestProperty("User-Agent", USER_AGENT);
    urlConnection.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
    urlConnection.setDoOutput(true);

    DataOutputStream wr = new DataOutputStream(urlConnection.getOutputStream());
    wr.writeBytes(getFragmentParameters(jsonFragment, recordId));
    wr.flush();
    wr.close();

    String record = null;
    try {
      InputStream in = new BufferedInputStream(urlConnection.getInputStream());
      record = readStream(in);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (urlConnection != null)
        urlConnection.disconnect();
    }
    return record;
  }

  private String readStream(InputStream in) throws IOException {
    BufferedReader rd = new BufferedReader(new InputStreamReader(in));

    StringBuffer result = new StringBuffer();
    String line = "";
    while ((line = rd.readLine()) != null) {
      result.append(line);
    }

    return result.toString();
  }
}
