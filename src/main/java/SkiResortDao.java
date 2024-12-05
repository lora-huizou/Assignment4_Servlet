import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import model.LiftRideEvent;
import model.SkierVerticalResponse;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SkiResortDao {
  private final DynamoDbClient ddb;
  private static final String TABLE_NAME = "SkiResortData";
  private static final String PARTITION_KEY = "skierID";
  private static final String SORT_KEY = "resortID#seasonID#dayID#time";
  private static final String GSI1_NAME = "GSI1"; // pk:resortID#seasonID#dayID#shardID, sk:skierID
  private static final String GSI1_PARTITION_KEY = "resortID#seasonID#dayID#shardID";
  private static final String GSI1_SORT_KEY = "skierID";
  private static final String GSI3_NAME = "GSI3"; // pk:skierID, sk:resortID#seasonID
  private static final String GSI3_SORT_KEY = "resortID#seasonID";
  private static final int TOTAL_SHARDS = 100;
  private LoadingCache<String, Integer> uniqueSkiersCache;
  private LoadingCache<String, Integer> skierDayVerticalCache;
  private LoadingCache<String, SkierVerticalResponse> skierVerticalCache;


  public SkiResortDao() {
    this.ddb = DynamoDbClient.builder()
        .region(Region.US_WEST_2)
        .credentialsProvider(InstanceProfileCredentialsProvider.create())
        .build();
    // Initialize the cache
    uniqueSkiersCache = CacheBuilder.newBuilder()
        .maximumSize(100) // Maximum cache size
        .expireAfterWrite(10, TimeUnit.MINUTES)
        .build(new CacheLoader<String, Integer>() {
          @Override
          public Integer load(String key) throws Exception {
            return loadUniqueSkiersFromDatabase(key);
          }
        });

    skierDayVerticalCache = CacheBuilder.newBuilder()
        .maximumSize(100)
        .expireAfterWrite(10, TimeUnit.MINUTES)
        .build(new CacheLoader<String, Integer>() {
          @Override
          public Integer load(String key) throws Exception {
            return loadSkierDayVerticalFromDatabase(key);
          }
        });

    skierVerticalCache = CacheBuilder.newBuilder()
        .maximumSize(100) // Maximum cache size
        .expireAfterWrite(10, TimeUnit.MINUTES) // Cache expiry
        .build(new CacheLoader<String, SkierVerticalResponse>() {
          @Override
          public SkierVerticalResponse load(String key) throws Exception {
            return loadSkierVerticalFromDatabase(key);
          }
        });
  }

  public void addLiftRidesBatch(List<LiftRideEvent> events) {
    int maxBatchSize = 25; // DynamoDB allows a maximum of 25 items per batch
    List<WriteRequest> currentBatch = new ArrayList<>();

    for (LiftRideEvent event : events) {
      Map<String, AttributeValue> item = createItem(event);
      currentBatch.add(
          WriteRequest.builder()
              .putRequest(PutRequest.builder().item(item).build())
              .build()
      );

      // If the current batch reaches the maximum size, process it
      if (currentBatch.size() == maxBatchSize) {
        processBatch(currentBatch);
        currentBatch.clear(); // Clear the batch for the next set of items
      }
    }

    // Process any remaining items in the current batch
    if (!currentBatch.isEmpty()) {
      processBatch(currentBatch);
    }
  }

  private void processBatch(List<WriteRequest> batch) {
    int retryCount = 0;
    boolean success = false;

    while (!success && retryCount < 5) {
      try {
        // Batch write request
        BatchWriteItemRequest batchRequest = BatchWriteItemRequest.builder()
            .requestItems(Map.of(TABLE_NAME, batch))
            .build();

        // Execute the batch write
        BatchWriteItemResponse response = ddb.batchWriteItem(batchRequest);

        // Check for unprocessed items and retry
        Map<String, List<WriteRequest>> unprocessedItems = response.unprocessedItems();
        if (unprocessedItems.isEmpty()) {
          success = true;
        } else {
          // Retry unprocessed items
          batch = unprocessedItems.get(TABLE_NAME);
          retryCount++;
          log.warn("Retrying {} unprocessed items (retry count: {})", batch.size(), retryCount);
          Thread.sleep(100 * retryCount); // Exponential backoff
        }
      } catch (DynamoDbException | InterruptedException e) {
        retryCount++;
        log.error("Error during batch write: ", e);
        try {
          Thread.sleep(100 * retryCount); // Exponential backoff
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    if (!success) {
      log.error("Batch write failed after {} retries.", retryCount);
    }
  }

  public void addLiftRide(LiftRideEvent event) {
    List<LiftRideEvent> batch = new ArrayList<>();
    batch.add(event);
    // Call the batch write method
    addLiftRidesBatch(batch);
  }

  private Map<String, AttributeValue> createItem(LiftRideEvent event) {
    // Compute shardID, to avoid hot partition
    int shardId = event.getSkierID() % TOTAL_SHARDS;

    // Main Table Sort Key: resortID#seasonID#dayID#time
    String sortKey = String.format("%d#%s#%d#%d",
        event.getResortID(), event.getSeasonID(), event.getDayID(), event.getLiftRide().getTime());

    // GSI1 Partition Key: resortID#seasonID#dayID#shardID
    String gsi1PartitionKey = String.format("%d#%s#%d#%d",
        event.getResortID(), event.getSeasonID(), event.getDayID(), shardId);

    // GSI3 Sort Key: resortID#seasonID
    String gsi3SortKey = String.format("%d#%s", event.getResortID(), event.getSeasonID());

    int vertical = event.getLiftRide().getLiftID() * 10;

    Map<String, AttributeValue> item = new HashMap<>();
    // Main table keys
    item.put(PARTITION_KEY, AttributeValue.builder() // Partition key
        .n(String.valueOf(event.getSkierID()))
        .build());
    item.put(SORT_KEY, AttributeValue.builder() // Sort key
        .s(sortKey)
        .build());

    // GSI 1 keys
    item.put(GSI1_PARTITION_KEY, AttributeValue.builder()
        .s(gsi1PartitionKey)
        .build());
    item.put(GSI1_SORT_KEY, AttributeValue.builder()
        .n(String.valueOf(event.getSkierID()))
        .build());
    // GSI 3 keys
    item.put("skierID", AttributeValue.builder()
        .n(String.valueOf(event.getSkierID()))
        .build());
    item.put(GSI3_SORT_KEY, AttributeValue.builder()
        .s(gsi3SortKey)
        .build());

    // Attributes
    item.put("resortId", AttributeValue.builder()
        .n(String.valueOf(event.getResortID()))
        .build());
    item.put("seasonID", AttributeValue.builder()
        .s(event.getSeasonID())
        .build());
    item.put("dayID", AttributeValue.builder()
        .n(String.valueOf(event.getDayID()))
        .build());
    item.put("time", AttributeValue.builder()
        .n(String.valueOf(event.getLiftRide().getTime()))
        .build());
    item.put("liftId", AttributeValue.builder()
        .n(String.valueOf(event.getLiftRide().getLiftID()))
        .build());
    item.put("vertical", AttributeValue.builder()
        .n(String.valueOf(vertical))
        .build());
    return item;
  }

  // GET/resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
  // Query 1: Get unique skiers for a resort/season/day
  private Integer loadUniqueSkiersFromDatabase(String key) {
    String[] parts = key.split("-");
    int resortID = Integer.parseInt(parts[0]);
    String seasonID = parts[1];
    int dayID = Integer.parseInt(parts[2]);

    return getUniqueSkiersFromDatabase(resortID, seasonID, dayID);
  }
  public int getUniqueSkiers(int resortID, String seasonID, int dayID) {
    String key = resortID + "-" + seasonID + "-" + dayID;

    // Use cache to retrieve the result
    try {
      return uniqueSkiersCache.get(key); // Returns from cache or loads from database if missing
    } catch (Exception e) {
      log.error("Error retrieving data from cache: ", e);
      return getUniqueSkiersFromDatabase(resortID, seasonID, dayID); // Fallback to database query
    }
  }
  public int getUniqueSkiersFromDatabase(int resortID, String seasonID, int dayID) {
    Set<Integer> uniqueSkiers = ConcurrentHashMap.newKeySet();
    ExecutorService executor = Executors.newFixedThreadPool(10); // Adjust thread pool size per performance

    List<Future<?>> futures = new ArrayList<>();

    for (int shardId = 0; shardId < TOTAL_SHARDS; shardId++) {
      final int shard = shardId;
      futures.add(executor.submit(() -> {
        String partitionKey = String.format("%d#%s#%d#%d", resortID, seasonID, dayID, shard);

        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#gsi1pk", GSI1_PARTITION_KEY); // "resortID#seasonID#dayID#shardID"

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":pk", AttributeValue.builder().s(partitionKey).build());

        QueryRequest queryRequest = QueryRequest.builder()
            .tableName(TABLE_NAME)
            .indexName(GSI1_NAME)
            .keyConditionExpression("#gsi1pk = :pk")
            .expressionAttributeNames(expressionAttributeNames)
            .expressionAttributeValues(expressionAttributeValues)
            .projectionExpression(GSI1_SORT_KEY)
            .build();
        QueryResponse response = ddb.query(queryRequest);
        for (Map<String, AttributeValue> item : response.items()) {
          int skierID = Integer.parseInt(item.get(GSI1_SORT_KEY).n());
          uniqueSkiers.add(skierID);
        }
      }));
    }

    // Wait for all tasks to complete
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        log.error("Error during getUniqueSkiers execution: ", e);
        Thread.currentThread().interrupt();
      }
    }
    executor.shutdown();
    return uniqueSkiers.size();
  }


  // GET/skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
  // Query 2: Get total vertical for a skier on a specific day
  public int getSkierDayVertical(int skierID, String seasonID, int dayID, int resortID) {
    // Cache key: dayID-skierID
    String key = String.format("day%d-skier%d", dayID, skierID);
    try {
      return skierDayVerticalCache.get(key); // Fetch from cache or load from database
    } catch (Exception e) {
      log.error("Error retrieving skier day vertical from cache: ", e);
      return getSkierDayVerticalFromDatabase(skierID, seasonID, dayID, resortID);
    }
  }

  private Integer loadSkierDayVerticalFromDatabase(String key) {
    // Parse the key
    String[] parts = key.split("-");
    int dayID = Integer.parseInt(parts[0].replace("day", ""));
    int skierID = Integer.parseInt(parts[1].replace("skier", ""));
    String seasonID = "2024"; // Constant per instructions
    int resortID = 1; // Constant per instructions
    return getSkierDayVerticalFromDatabase(skierID, seasonID, dayID, resortID);
  }

  public int getSkierDayVerticalFromDatabase(int skierID, String seasonID, int dayID, int resortID) {
    String sortKeyPrefix = String.format("%d#%s#%d#", resortID, seasonID, dayID);

    Map<String, String> expressionAttributeNames = new HashMap<>();
    expressionAttributeNames.put("#partitionKey", PARTITION_KEY); // "skierID"
    expressionAttributeNames.put("#sortKey", SORT_KEY); // "resortID#seasonID#dayID#time"

    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    expressionAttributeValues.put(":skierID", AttributeValue.builder().n(String.valueOf(skierID)).build());
    expressionAttributeValues.put(":skPrefix", AttributeValue.builder().s(sortKeyPrefix).build());

    QueryRequest queryRequest = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("#partitionKey = :skierID AND begins_with(#sortKey, :skPrefix)")
        .expressionAttributeNames(expressionAttributeNames)
        .expressionAttributeValues(expressionAttributeValues)
        .projectionExpression("vertical")
        .build();

    QueryResponse response = ddb.query(queryRequest);

    int totalVertical = response.items().stream()
        .mapToInt(item -> Integer.parseInt(item.get("vertical").n()))
        .sum();

    return totalVertical;
  }

  // Query 3: Get the total vertical for the skier the specified resort. If no season is specified, return all seasons
  // GET/skiers/{skierID}/vertical
  public SkierVerticalResponse getTotalVertical(int skierID, String resort, String season) {
    if (resort == null || resort.isEmpty()) {
      throw new IllegalArgumentException("Resort is a required parameter");
    }

    String cacheKey = (season != null && !season.isEmpty())
        ? String.format("totalVertical:%d:%s:%s", skierID, resort, season)
        : String.format("totalVertical:%d:%s:all", skierID, resort);

    try {
      // Use cache to get the response
      return skierVerticalCache.get(cacheKey);
    } catch (ExecutionException e) {
      log.error("Error loading skier vertical from cache: {}", e.getMessage());
      throw new RuntimeException("Failed to fetch skier vertical", e);
    }
  }
  private SkierVerticalResponse loadSkierVerticalFromDatabase(String key) {
    String[] parts = key.split(":");
    int skierID = Integer.parseInt(parts[1]);
    String resort = parts[2];
    String season = parts.length == 4 ? parts[3] : null;

    Map<String, String> expressionAttributeNames = new HashMap<>();
    expressionAttributeNames.put("#partitionKey", PARTITION_KEY); // "skierID"
    expressionAttributeNames.put("#gsi3SortKey", GSI3_SORT_KEY); // "resortID#seasonID"

    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    expressionAttributeValues.put(":skierID", AttributeValue.builder().n(String.valueOf(skierID)).build());

    String keyConditionExpression = "#partitionKey = :skierID";

    List<SkierVerticalResponse.SeasonVertical> seasonVerticals = new ArrayList<>();

    if (season != null && !season.isEmpty()) {
      // Query for a specific season
      String sortKey = resort + "#" + season;
      expressionAttributeValues.put(":sortKey", AttributeValue.builder().s(sortKey).build());
      keyConditionExpression += " AND #gsi3SortKey = :sortKey";

      QueryRequest queryRequest = QueryRequest.builder()
          .tableName(TABLE_NAME)
          .indexName(GSI3_NAME)
          .keyConditionExpression(keyConditionExpression)
          .expressionAttributeNames(expressionAttributeNames)
          .expressionAttributeValues(expressionAttributeValues)
          .projectionExpression("vertical, seasonID")
          .build();

      QueryResponse response = ddb.query(queryRequest);

      int totalVertical = response.items().stream()
          .mapToInt(item -> Integer.parseInt(item.get("vertical").n()))
          .sum();

      seasonVerticals.add(new SkierVerticalResponse.SeasonVertical(season, totalVertical));

    } else {
      // Query for all seasons
      String sortKeyPrefix = resort + "#";
      expressionAttributeValues.put(":sortKeyPrefix", AttributeValue.builder().s(sortKeyPrefix).build());
      keyConditionExpression += " AND begins_with(#gsi3SortKey, :sortKeyPrefix)";

      QueryRequest queryRequest = QueryRequest.builder()
          .tableName(TABLE_NAME)
          .indexName(GSI3_NAME)
          .keyConditionExpression(keyConditionExpression)
          .expressionAttributeNames(expressionAttributeNames)
          .expressionAttributeValues(expressionAttributeValues)
          .projectionExpression("vertical, seasonID")
          .build();

      QueryResponse response = ddb.query(queryRequest);

      // Group by seasonID and sum verticals
      Map<String, Integer> seasonTotals = new HashMap<>();
      for (Map<String, AttributeValue> item : response.items()) {
        String seasonID = item.get("seasonID").s();
        int vertical = Integer.parseInt(item.get("vertical").n());
        seasonTotals.put(seasonID, seasonTotals.getOrDefault(seasonID, 0) + vertical);
      }

      // Create SeasonVertical objects
      for (Map.Entry<String, Integer> entry : seasonTotals.entrySet()) {
        seasonVerticals.add(new SkierVerticalResponse.SeasonVertical(entry.getKey(), entry.getValue()));
      }
    }

    return new SkierVerticalResponse(seasonVerticals);
  }


}

