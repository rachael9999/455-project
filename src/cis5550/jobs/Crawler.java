package cis5550.jobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.net.URLEncoder;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.URLParser;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;

public class Crawler {
  static List<String> seedList = new ArrayList<>();
  static String kvsHost = "localhost:8000";
  static String userAgent = "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36";
  // static String userAgent = "Googlebot";

  static int crawlDelay = 100;
  private static final Pattern URL_PATTERN = Pattern.compile("<a\\s+(?:[^>]*?\\s+)?href=([\"'])(.*?)\\1", Pattern.CASE_INSENSITIVE);
  private static final int BACKOFF_DELAY = 5000;
  private static final int MAX_RETRIES = 10;
  
  static Logger logger = Logger.getLogger(Crawler.class);

  // static Set<String> seenUrls = ConcurrentHashMap.newKeySet();
  static Map<String, Long> lastAccessTimes = new ConcurrentHashMap<>();
  static Map<String, Map<String, List<String>>> robotMap = new HashMap<>();
  static Map<String, Integer> crawlDelayMap = new HashMap<>();
  static boolean fetch = false;
  static KVSClient kvsClient = new KVSClient(kvsHost);
  static List<String> blackList = new ArrayList<>();
  static Set<String> newUrls = ConcurrentHashMap.newKeySet();
  static Set<String> delayedUrls = ConcurrentHashMap.newKeySet();

  static SSLSocketFactory sslSocketFactory;

  static {
    try {
      TrustManager[] trustAllCerts = new TrustManager[] {
        new X509TrustManager() {
          public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
          }
          public void checkClientTrusted(
            java.security.cert.X509Certificate[] certs, String authType) {
          }
          public void checkServerTrusted(
            java.security.cert.X509Certificate[] certs, String authType) {
          }
        }
      };

        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        sslSocketFactory = sc.getSocketFactory();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  

  public static void run(FlameContext context, String[] args) {
    kvsClient = context.getKVS();
    if (args.length < 1 || args.length > 2) {
      context.output("Usage: crawler <seed> <blacklist>, blacklist is optional");
      return;
    } else {

      // if (args.length == 2) {
      //   String blacklistTable = args[1];
      //   context.output("Using blacklist file: " + blacklistTable);
      //   try {
      //     Iterator<Row> rows = kvsClient.scan(blacklistTable);
      //     while (rows.hasNext()) {
      //       Row row = rows.next();
      //       blackList.add(row.get("pattern"));
      //     }
      //   } catch (Exception e) {
      //     e.printStackTrace();
      //     return;
      //   }
      // }

      String seed = args[0];
      context.output("Crawing from seed url: " + seed);
      try {
        seed = normalizeUrl(seed, seed);
        if (seed == null) {
          return;
        }

      } catch (Exception e) {
        logger.error("Error normalizing seed url: " + seed );
        e.printStackTrace();
        return;
      }

      newUrls.add(seed);

      FlameRDD prUrls = null;
      try {
        prUrls = context.fromTable("pt-urls", (Row r) -> {
          try {
            if (kvsClient.existsRow("pt-crawl", r.key())) {
              if (kvsClient.get("pt-crawl", r.key(), "page") != null) {
                return null;
              }
            }
            newUrls.add(r.get(r.key()));
          } catch (Exception e) {
            logger.error("Error reading from pt-urls table");
            e.printStackTrace();
          }
          return null;
        });
      } catch (Exception e) {
        logger.error("Error reading from pt-urls table");
        e.printStackTrace();
      }

      

      try {
        FlameRDD urls = context.parallelize(new ArrayList<>(newUrls));
        while (urls.count() > 0) {
          urls = urls.flatMap(url -> {
            
            try {

              String host = URLParser.parseURL(url)[1];
              Long lastAccessTime = lastAccessTimes.get(host);

              crawlDelay = crawlDelayMap.getOrDefault(userAgent.toLowerCase(), crawlDelayMap.getOrDefault(("*"), 100));

              if (lastAccessTime != null && System.currentTimeMillis() - lastAccessTime < crawlDelay) {
                newUrls.remove(url);
                delayedUrls.add(url);
                return newUrls;
              }
            } catch (Exception e) {
              e.printStackTrace();
              return newUrls;
            }

            String hashUrl = Hasher.hash(url);

            if (kvsClient.existsRow("pt-crawl", hashUrl)) {
              if (kvsClient.get("pt-crawl", hashUrl, "page") != null) {
                return newUrls;
              }
            }

            boolean http = url.startsWith("http://");

            String headResponse = headRequest(url, kvsClient, newUrls, http);
          
            if (headResponse.equals("empty")) {
              return newUrls;
            }

            // System.out.println(newUrls);
            newUrls.addAll(delayedUrls);
            return newUrls;
          });
          
          // urls.saveAsTable("urls");
        }
        

      } catch (Exception e) {
        logger.error("Error crawling from seed: " + seed);
        e.printStackTrace();
      }
    }
  }

  private static boolean fetchRobotsTxt(String host) {
    try {

      URL url = new URL("http", host, 80, "/robots.txt");
      // System.out.println("Fetching robots.txt for " + host + " at " + url);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("User-Agent", userAgent);
      int responseCode = connection.getResponseCode();

      if (responseCode == 200) {
        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String inputLine;
        String currentUserAgent = null;
        Map<String, List<String>> rulesMap = new HashMap<>();
        while ((inputLine = in.readLine()) != null) {
          if (inputLine.toLowerCase().startsWith("user-agent:")) {
            currentUserAgent = inputLine.substring(11).trim().toLowerCase();
            rulesMap = new HashMap<>();
          } else if (inputLine.toLowerCase().startsWith("disallow:")
              || inputLine.toLowerCase().startsWith("allow:")) {
            if (currentUserAgent != null) {
              String ruleType = inputLine.substring(0, inputLine.indexOf(":")).trim().toLowerCase();
              String rule = inputLine.substring(inputLine.indexOf(":") + 1).trim();
              List<String> rules = rulesMap.getOrDefault(ruleType, new ArrayList<>());
              rules.add(rule);
              rulesMap.put(ruleType, rules);
            }

          } else if (inputLine.toLowerCase().startsWith("crawl-delay:")) {
            Double delay = Double.parseDouble(inputLine.substring(12).trim());
            int delayInt = (int) Math.ceil(delay * 1000);
            crawlDelayMap.put(currentUserAgent, delayInt);
          } else if (inputLine.length() == 0) {
            if (currentUserAgent != null) {
              robotMap.put(currentUserAgent, rulesMap);
            }
          }
          robotMap.put(currentUserAgent, rulesMap);
        }
      }
      System.out.println(robotMap + "Host: " + host);

      // try (ObjectOutputStream out = new ObjectOutputStream(new
      // FileOutputStream(file))) {
      // out.writeObject(robotMap);
      // // System.out.println(file.getAbsolutePath());
      // } catch (IOException e) {
      // e.printStackTrace();
      // }

    } catch (Exception e) {
      logger.error("Error fetching robot text" + host);
      e.printStackTrace();
    }

    // System.out.println("Fetched robots.txt for " + host + robotMap);
    fetch = true;
    return true;
  }

  static boolean isAllowed(String path, String userAgent) {
    Map<String, List<String>> rules = robotMap.get(userAgent.toLowerCase());
    if (rules == null) {
      rules = robotMap.get("*");
    }

    if (rules == null) {
      return true;
    }

    // Split the path into components, removing any leading /
    String[] pathComponents = path.replaceFirst("^/", "").split("/");

    // Process the "allow" and "disallow" rules separately
    for (String ruleType : new String[] {"disallow", "allow"}) {
        List<String> ruleList = rules.getOrDefault(ruleType, new ArrayList<>());

        for (String rule : ruleList) {
            String rulePath = rule.substring(rule.indexOf(":") + 1).trim();

            // Split the rule path into components, removing any leading /
            String[] ruleComponents = rulePath.replaceFirst("^/", "").split("/");

            // If the rule path components are a prefix of the path components
            if (isPrefix(ruleComponents, pathComponents)) {
                return "allow".equals(ruleType);
            }
        }
    }

    // If no rules match, allow the path
    return true;
}

// Helper method to check if one array of strings is a prefix of another
static boolean isPrefix(String[] prefix, String[] array) {
    if (prefix.length > array.length) {
        return false;
    }

    for (int i = 0; i < prefix.length; i++) {
        if (!prefix[i].equals(array[i])) {
            return false;
        }
    }

    return true;
}

  public static List<String> extractUrls(String line) {
    List<String> urls = new ArrayList<>();
    // . matches any character except line terminators
    Matcher matcher = URL_PATTERN.matcher(line);
    while (matcher.find()) {
      urls.add(matcher.group(2));
    }
    if (urls.size() > 15) {
      Collections.shuffle(urls);
      urls = urls.subList(0, 15);
    }
    return urls;
  }

  public static String normalizeUrl(String baseUrl, String url) throws MalformedURLException, URISyntaxException {

    String[] parsedUrl = URLParser.parseURL(url);
    String protocol = parsedUrl[0];
    String host = parsedUrl[1];
    String port = parsedUrl[2];
    String path = parsedUrl[3];

    // if the url is relative
    if (host == null) {
      String[] baseParsedUrl = URLParser.parseURL(baseUrl);
      String baseProtocol = baseParsedUrl[0];
      String baseHost = baseParsedUrl[1];
      String basePort = baseParsedUrl[2];
      String basePath = baseParsedUrl[3];

      protocol = baseProtocol;
      host = baseHost;
      port = basePort;

      // If path doesn't start with '/', it's relative to the current directory
      if (!path.startsWith("/")) {
        // Remove the last segment of basePath
        int lastSlashIndex = basePath.lastIndexOf("/");
        if (lastSlashIndex != -1) {
          basePath = basePath.substring(0, lastSlashIndex);
        }

        // Handle '..' in the path
        while (path.startsWith("../")) {
          path = path.substring(3);
          lastSlashIndex = basePath.lastIndexOf("/");
          if (lastSlashIndex != -1) {
            basePath = basePath.substring(0, lastSlashIndex);
          }
        }

        path = basePath + "/" + path;
      }
      // If path starts with '/', it's relative to the root
      else {
        basePath = "";
        path = basePath + path;
      }
    }

    // Remove fragment from the URL
    int hashIndex = path.indexOf("#");
    if (hashIndex != -1) {
      path = path.substring(0, hashIndex);
    }

    if (port == null) {
      if ("http".equals(protocol)) {
        port = "80";
      } else if ("https".equals(protocol)) {
        port = "443";
      } else {
        return null;
      }
    }

    if (robotMap.isEmpty() && !fetch) {
      fetchRobotsTxt(host);
    }

    boolean isAllowed = isAllowed(path, userAgent);
    // System.out.println("Is allowed: " + isAllowed + " for path: " + path + " and
    // user agent: " + userAgent + robotMap);
    // System.out.println("Robot map: " + robotMap);

    if (!isAllowed) {
      // System.out.println("Not allowed to crawl " + path + " for user agent " +
      // userAgent);
      return null;
    }

    URL normalizedUrl = new URL(protocol, host, Integer.parseInt(port), path);
    if (!normalizedUrl.toString().startsWith("http://") && !normalizedUrl.toString().startsWith("https://")) {
      // System.out.println("Not HTTP or HTTPS request " + path + " for user agent " +
      // userAgent);
      return null;
    } else if (normalizedUrl.toString().endsWith(".jpg")
        || normalizedUrl.toString().endsWith(".jpeg")
        || normalizedUrl.toString().endsWith(".gif")
        || normalizedUrl.toString().endsWith(".png")
        || normalizedUrl.toString().endsWith(".txt")) {
      // System.out.println("Not HTML request " + path + " for user agent " +
      // userAgent);
      return null;
    }

    // System.out.println("Normalized URL: " + normalizedUrl.toString());
    return normalizedUrl.toString();
  }




  public static void getRequest(String url, Set<String> newUrls, boolean http, KVSClient kvsClient) {

    int retryCount = 0;
    while (retryCount < MAX_RETRIES) {
      try {
        List<String> extractedUrls = new ArrayList<>();
        List<String> normalizedUrls = new ArrayList<>();
        StringBuilder line = new StringBuilder();
        StringBuilder page = new StringBuilder();

        URL urlObj = new URL(url);
        HttpURLConnection connection;
        if (http) {
          connection = (HttpURLConnection) urlObj.openConnection();
        } else {
          connection = (HttpsURLConnection) urlObj.openConnection();
          ((HttpsURLConnection) connection).setSSLSocketFactory(sslSocketFactory);
        }
        connection.setRequestMethod("GET");
        connection.setRequestProperty("User-Agent", userAgent);
        connection.setRequestProperty("Cookie", "countryCode=US");
        connection.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
        connection.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
        connection.setRequestProperty("Accept-Encoding", "gzip, deflate");
        connection.setRequestProperty("Connection", "keep-alive");
        connection.setConnectTimeout(5000); 
        connection.setReadTimeout(5000); 
        connection.connect();

        int responseCode = connection.getResponseCode();
        String contentType = connection.getContentType();
        int contentLength = connection.getContentLength();
        String contentEncoding = connection.getContentEncoding();
        InputStream inputStream = connection.getInputStream();

        if (responseCode == 429) {
          String host = new URL(url).getHost();
    
          String retryAfter = connection.getHeaderField("Retry-After");
          long delay = retryAfter != null ? Long.parseLong(retryAfter) * 1000 : BACKOFF_DELAY;
    
          try {
            Thread.sleep(delay);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
          }
          retryCount++;
          continue;
        }

        if ("gzip".equalsIgnoreCase(contentEncoding)) {
          inputStream = new GZIPInputStream(inputStream, 1024 * 1024);
        } else if ("deflate".equalsIgnoreCase(contentEncoding)) {
          inputStream = new InflaterInputStream(inputStream);
        } else {
          inputStream = connection.getInputStream();
        
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));

        lastAccessTimes.put(URLParser.parseURL(url)[1], System.currentTimeMillis());

        String hashUrl = Hasher.hash(url);
        Row row = new Row(hashUrl);

        if (responseCode == 200) {

          row.put("url", url);
          row.put("responseCode", Integer.toString(responseCode));
    
          if (contentType != null) {
            row.put("contentType", contentType);
          }
    
          if (contentLength != -1) {
            row.put("length", Integer.toString(contentLength));
          }
        }
        
        char[] buffer = new char[1024];
        int numCharsRead;
        
        while ((numCharsRead = reader.read(buffer, 0, buffer.length)) != -1) {
          page.append(buffer, 0, numCharsRead);
        }
        reader.close();
        
        String pageString = page.toString();
        List<String> urls = extractUrls(pageString);
        extractedUrls.addAll(urls);

        for (String u : extractedUrls) {
          String normalizedUrl = normalizeUrl(url, u);
          if (normalizedUrl == null) {
            continue;
          }

          if (!blackList.isEmpty()) {
            for (String pattern : blackList) {
              String regex = pattern.replace("*", ".*");
              if (normalizedUrl.matches(pattern)) {
                continue;
              }
            }
          }

          normalizedUrls.add(normalizedUrl);
          

          // System.out.println("exist" + kvsClient.get("pt-crawl", "url",
          // Hasher.hash(normalizedUrl)) != null);


          if (kvsClient.existsRow("pt-crawl", Hasher.hash(normalizedUrl))) {
            if (kvsClient.get("pt-crawl", Hasher.hash(url), "page") != null) {
              continue;
            }
          }
          newUrls.add(normalizedUrl);

        }

        row.put("page", pageString);
        
        kvsClient.putRow("pt-crawl", row);
        return;

      } catch (SocketTimeoutException e) {
        logger.error("Socket timeout for url: " + url);
        e.printStackTrace();
        retryCount++;
        continue;
      } catch (MalformedURLException e) {
        logger.error("Malformed url: " + url);
        e.printStackTrace();
        break;
      } catch (IOException e) {
        logger.error("Error getting request for url: " + url);
        e.printStackTrace();
        break;
      } catch (Exception e) {
        logger.error("Error getting request for url: " + url);
        e.printStackTrace();
        return;
      }
    }
    if (retryCount == MAX_RETRIES) {
      // If the maximum number of retries was reached, handle accordingly
      System.out.println("Maximum number of retries reached" + url);
      return;
    }
  }

  protected static String headRequest(String url, KVSClient kvsClient, Set<String> newUrls, boolean http) {

    int retryCount = 0;
    int MAX_REDIRECTS = 5; 
    int redirectCount = 0;  

    if (url.startsWith("https://archive-it.org:443/")) {
      return "empty";
    }
    while (retryCount < MAX_RETRIES) {
      try {
        HttpURLConnection connection;
        URL urlObj = new URL(url);
        if (http) {
          connection = (HttpURLConnection) urlObj.openConnection();
        } else {
          connection = (HttpsURLConnection) urlObj.openConnection();
          ((HttpsURLConnection) connection).setSSLSocketFactory(sslSocketFactory);
        }
        connection.setRequestMethod("HEAD");
        connection.setRequestProperty("User-Agent", userAgent);
        connection.setRequestProperty("Cookie", "countryCode=US");
        connection.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
        connection.setRequestProperty("Connection", "keep-alive");
        connection.setConnectTimeout(5000); 
        connection.setReadTimeout(5000); 
        connection.setInstanceFollowRedirects(false);
        connection.connect();

        int headResponseCode = connection.getResponseCode();
        String contentType = connection.getContentType();
        int contentLength = connection.getContentLength();

      
        if (headResponseCode == 429) {
          String host = new URL(url).getHost();

          String retryAfter = connection.getHeaderField("Retry-After");
          long delay = retryAfter != null ? Long.parseLong(retryAfter) * 1000 : BACKOFF_DELAY;
        
          try {
            Thread.sleep(delay);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
          }
          retryCount++;
          continue;
        }
      

        // If the response is neither OK (200) nor a redirect (301, 302, 303, 307, 308),
        // return an empty set
        if (!(headResponseCode == 200 || (headResponseCode >= 301 && headResponseCode <= 303) || headResponseCode == 307
        || headResponseCode == 308)) {
          return "empty";
        }

        // If the response is a redirect (301, 302, 303, 307, 308), store the redirect
        // location

        while (headResponseCode >= 301
            && headResponseCode <= 303
            || headResponseCode == 307
            || headResponseCode == 308) {
          String location = connection.getHeaderField("Location");
          if (location == null) {
            return "empty";
          }
          location = normalizeUrl(url, location);
          if (location == null) {
            return "empty";
          }
          url = location;
          connection = (HttpURLConnection) new URL(url).openConnection();
          connection.setRequestMethod("HEAD");
          connection.setRequestProperty("User-Agent", userAgent);
          connection.setRequestProperty("Cookie", "countryCode=US");
          connection.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
          connection.setRequestProperty("Connection", "keep-alive");
          connection.setConnectTimeout(5000);
          connection.setReadTimeout(5000);
          connection.setInstanceFollowRedirects(false);
          connection.connect();
          headResponseCode = connection.getResponseCode();

          redirectCount++;
          if (redirectCount > MAX_REDIRECTS) {
            return "empty";
          }
        }
        // if (headResponseCode >= 301
        //     && headResponseCode <= 303
        //     || headResponseCode == 307
        //     || headResponseCode == 308) {
        //   String hashUrl = Hasher.hash(url);
        //   Row row = new Row(hashUrl);
        //   row.put("url", url);
        //   row.put("responseCode", Integer.toString(headResponseCode));

        //   String location = connection.getHeaderField("Location");

        //   // System.out.println("original url " + url + "redirects to " + location);
        //   if (location == null) {
        //     return "empty";
        //   }
        //   location = normalizeUrl(url, location);
        //   if (location == null) {
        //     return "empty";
        //   }
        //   kvsClient.putRow("pt-crawl", row);
        //   newUrls.offer(location);
        //   return "redirect";
        // }

        if (headResponseCode == 200 && contentType != null && contentType.contains("text/html")) {
          getRequest(url, newUrls, http, kvsClient);
          return "200";
        }
        // System.out.println("All the other cases: " + url + " " + headResponseCode + "
        // " + contentType);
        return "empty";
    } catch (SocketTimeoutException e) {
      logger.error("Socket timeout for url: " + url);
      e.printStackTrace();
      retryCount++;
      continue;
    } catch (MalformedURLException e) {
      logger.error("Malformed url: " + url);
      e.printStackTrace();
      break;
    }  catch (IOException e) {
      logger.error("Error head request for url: " + url);
      e.printStackTrace();
      break;
    } catch (Exception e) {
      logger.error("Error head request for url: " + url);
      e.printStackTrace();
      return "empty";
    }


    }
    if (retryCount == MAX_RETRIES) {
      // If the maximum number of retries was reached, handle accordingly
      System.out.println("Maximum number of retries reached");
      return "empty";
    }
    
    return "empty";
  }

}
