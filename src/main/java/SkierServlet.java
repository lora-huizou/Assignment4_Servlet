import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.annotation.*;
import java.io.IOException;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import lombok.extern.slf4j.Slf4j;
import model.LiftRide;
import model.LiftRideEvent;
import model.ResponseMsg;
import com.rabbitmq.client.Connection;
import model.SkierVerticalResponse;

@Slf4j
@WebServlet(value = {"/skiers/*", "/resorts/*"})
public class SkierServlet extends HttpServlet {
  private ResponseMsg responseMsg = new ResponseMsg();
  private Gson gson = new Gson();
  private Connection connection;
  private RMQChannelPool channelPool;
  private static final String QUEUE_NAME = "LiftRideQueue";
  private static final Integer CHANNEL_POOL_SIZE = 100;

  private SkiResortDao dao;

  private static final String HOST = "54.190.212.169"; // RabbitMQ server IP
  //private static final String HOST = "localhost";

  @Override
  public void init() throws ServletException {
    super.init();
    // Initialize RabbitMQ connection
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(HOST);
    factory.setPort(5672);
    factory.setUsername("guest");
    factory.setPassword("guest");

    try {
      connection = factory.newConnection();
      RMQChannelFactory channelFactory = new RMQChannelFactory(connection);
      // init channel pool
      channelPool = new RMQChannelPool(CHANNEL_POOL_SIZE, channelFactory);

      // Declare the queue only once during initialization
      try (Channel setupChannel = connection.createChannel()) {
        setupChannel.queueDeclare(QUEUE_NAME, true, false, false, null);
      }
      // Initialize SkiResortDao
      dao = new SkiResortDao();
      log.info("SkiResortDao initialized successfully");
    } catch (IOException | TimeoutException e) {
      throw new ServletException("Failed to establish RabbitMQ connection", e);
    }
  }
  @Override
  public void destroy() {
    super.destroy();
    try {
      if (channelPool != null) {
        channelPool.close();
      }
      if (connection != null && connection.isOpen()) {
        connection.close();
      }
    } catch (IOException e) {
      log.error("destroy error:{}", e.getMessage());
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    res.setContentType("text/plain");
    String urlPath = req.getPathInfo();

    // Check if we have a URL
    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      responseMsg.setMessage("Missing parameters");
      res.getWriter().write(gson.toJson(responseMsg));
      return;
    }

    String[] urlParts = urlPath.split("/");
    try {
      // GET /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
      //          ["", "1", "seasons", "2024", "day", "1", "skiers"]
      if (urlParts.length == 7 && urlParts[0].equals("") && urlParts[2].equals("seasons") && urlParts[4].equals("day") && urlParts[6].equals("skiers") ) {
        handleGetUniqueSkiers(req, res, urlParts);
      } else if (urlParts.length == 8 && urlParts[2].equals("seasons") && urlParts[4].equals("days") && urlParts[6].equals("skiers")) {
        // GET /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
        handleGetSkierDayVertical(req, res, urlParts);
      } else if (urlParts.length == 3 && urlParts[0].equals("") && urlParts[2].equals("vertical")) {
        // GET /skiers/{skierID}/vertical
        handleGetTotalVertical(req, res, urlParts);
      } else {
        res.setStatus(HttpServletResponse.SC_NOT_FOUND);
        responseMsg.setMessage("Invalid URL");
        res.getWriter().write(gson.toJson(responseMsg));
      }
    } catch (NumberFormatException e) {
      sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST, "Invalid numerical parameter");
    } catch (IllegalArgumentException e) {
      sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      sendErrorResponse(res, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Server error: " + e.getMessage());
      log.error("Error processing GET request", e);
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    // {"resortID":12,"seasonID":"2019","dayID":1,"skierID":123,"liftRide":{"liftID":10,"time":277}}
    res.setContentType("application/json");
    String urlPath = req.getPathInfo();

    // Check if we have a URL
    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      responseMsg.setMessage("Missing paramterers");
      res.getWriter().write(gson.toJson(responseMsg));
      return;
    }

    String[] urlParts = urlPath.split("/");

    // Validate URL path and return the response status code
    if (!isUrlValid(urlParts)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      responseMsg.setMessage("Invalid URL");
      res.getWriter().write(gson.toJson(responseMsg));
      return;
    }
    // read and parse JSON
    StringBuilder jsonBody = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(req.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        jsonBody.append(line);
      }
    }
    LiftRide liftRide = null;
    try {
      liftRide = gson.fromJson(jsonBody.toString(), LiftRide.class);
    } catch (JsonSyntaxException e) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      responseMsg.setMessage("Invalid JSON format");
      res.getWriter().write(gson.toJson(responseMsg));
      return;
    }

    if (liftRide == null || liftRide.getLiftID() < 1 || liftRide.getLiftID() > 40 || liftRide.getTime() < 1 || liftRide.getTime() > 360) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      responseMsg.setMessage("Invalid LiftRide data");
      res.getWriter().write(gson.toJson(responseMsg));
      return;
    }
    //parse from url
    int resortID = Integer.parseInt(urlParts[1]);
    String seasonID = urlParts[3];
    int dayID = Integer.parseInt(urlParts[5]);
    int skierID = Integer.parseInt(urlParts[7]);
    LiftRideEvent liftRideMessage = new LiftRideEvent(resortID, seasonID, dayID, skierID, liftRide);

    Channel channel = null;
    try {
      channel = channelPool.borrowChannel();
      // declare the queue
      //channel.queueDeclare(QUEUE_NAME, true, false, false, null);
      String message = gson.toJson(liftRideMessage);
      // publish message to queue
      channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
      // return success to client
      res.setStatus(HttpServletResponse.SC_CREATED);
      responseMsg.setMessage("POST Request Processed Successfully!");
      res.getWriter().write(gson.toJson(responseMsg));
    } catch (IOException e){
      res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      responseMsg.setMessage("Failed to process request");
      res.getWriter().write(gson.toJson(responseMsg));
    } finally {
      if (channel != null){
        try {
          channelPool.returnChannel(channel);
        } catch (Exception e){
          log.error("Error returning channel to pool: {}", e.getMessage());
        }
      }
    }
  }
  private boolean isUrlValid(String[] urlParts) {
    // given url : /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
    // urlParts  = "/1/seasons/2019/day/1/skier/123"
    // urlParts = [, 1, seasons, 2019, days, 1, skier, 123]
    if (urlParts.length != 8) {
      return false;
    }
    if (!urlParts[2].equals("seasons") || !urlParts[4].equals("days") || !urlParts[6].equals("skiers")) {
      return false;
    }

    try {
      int resortID = Integer.parseInt(urlParts[1]);
      if (resortID < 1) {
        return false;
      }

      String seasonID = urlParts[3];
      if (seasonID.length() != 4 || !seasonID.matches("\\d{4}")) {
        return false;
      }

      int dayID = Integer.parseInt(urlParts[5]);
      if (dayID < 1 || dayID > 366) {
        return false;
      }

      int skierID = Integer.parseInt(urlParts[7]);
      if (skierID < 1) {
        return false;
      }
    } catch (NumberFormatException ex) {
      // If any of the path variables are not integers, return false
      return false;
    }
    return true;
  }

  private void handleGetUniqueSkiers(HttpServletRequest req, HttpServletResponse res, String[] urlParts) throws IOException {
    // GET /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
    int resortID = Integer.parseInt(urlParts[1]);
    String seasonID = urlParts[3];
    int dayID = Integer.parseInt(urlParts[5]);
    int uniqueSkiers = dao.getUniqueSkiers(resortID, seasonID, dayID);
    responseMsg.setMessage("Unique skiers: " + uniqueSkiers);
    sendJsonResponse(res, HttpServletResponse.SC_OK, responseMsg);
  }

  private void handleGetSkierDayVertical(HttpServletRequest req, HttpServletResponse res, String[] urlParts) throws IOException {
    //GET /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
    int resortID = Integer.parseInt(urlParts[1]);
    String seasonID = urlParts[3];
    int dayID = Integer.parseInt(urlParts[5]);
    int skierID = Integer.parseInt(urlParts[7]);
    int totalVertical = dao.getSkierDayVertical(skierID, seasonID, dayID, resortID);
    responseMsg.setMessage("Total vertical: " + totalVertical);
    sendJsonResponse(res, HttpServletResponse.SC_OK, responseMsg);
  }

  private void handleGetTotalVertical(HttpServletRequest req, HttpServletResponse res, String[] urlParts) throws IOException {
    //GET /skiers/{skierID}/vertical
    int skierID = Integer.parseInt(urlParts[1]);
    String resort = req.getParameter("resort");
    String season = req.getParameter("season");

    if (resort == null || resort.isEmpty()) {
      throw new IllegalArgumentException("Missing required parameter: resort");
    }
    SkierVerticalResponse skierVerticalResponse = dao.getTotalVertical(skierID, resort, season);
    sendJsonResponse(res, HttpServletResponse.SC_OK, skierVerticalResponse);
  }

  private void sendErrorResponse(HttpServletResponse res, int statusCode, String message) throws IOException {
    res.setStatus(statusCode);
    responseMsg.setMessage(message);
    res.getWriter().write(gson.toJson(responseMsg));
  }
  private void sendJsonResponse(HttpServletResponse res, int statusCode, Object response) throws IOException {
    res.setStatus(statusCode);
    res.getWriter().write(gson.toJson(response));
  }

}
