import java.io.BufferedReader;
import java.io.InputStreamReader;
import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.annotation.*;
import java.io.IOException;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import model.LiftRide;
import model.ResponseMsg;

@WebServlet(value = "/skiers/*")
public class SkierServlet extends HttpServlet {
  private ResponseMsg responseMsg = new ResponseMsg();
  private Gson gson = new Gson();

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    res.setContentType("text/plain");
    String urlPath = req.getPathInfo();

    // check we have a URL!
    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      responseMsg.setMessage("Missing paramterers");
      res.getWriter().write(gson.toJson(responseMsg));
      return;
    }

    String[] urlParts = urlPath.split("/");
    // and now validate url path and return the response status code
    // (and maybe also some value if input is valid)

    if (!isUrlValid(urlParts)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      responseMsg.setMessage("Invalid url");
      res.getWriter().write(gson.toJson(responseMsg));
    } else {
      // do any sophisticated processing with urlParts which contains all the url params
      int resortID = Integer.parseInt(urlParts[1]);
      String seasonID = urlParts[3];
      String dayID = urlParts[5];
      int skierID = Integer.parseInt(urlParts[7]);
      responseMsg.setMessage(String.format("Lift ride value get successfully for, "
          + "resortID %d, seasonID %s, dayID %s, skierID %d",resortID, seasonID, dayID, skierID ));
      res.getWriter().write(gson.toJson(responseMsg));
      res.setStatus(HttpServletResponse.SC_OK);
    }
  }

  protected void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
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
    LiftRide liftRide;
    try {
      liftRide = gson.fromJson(jsonBody.toString(), LiftRide.class);
    } catch (JsonSyntaxException e) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      responseMsg.setMessage("Invalid JSON format");
      res.getWriter().write(gson.toJson(responseMsg));
      return;
    }

    if (liftRide.getLiftID() < 1 || liftRide.getLiftID() > 40 || liftRide.getTime() < 1 || liftRide.getTime() > 360) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      responseMsg.setMessage("Invalid LiftRide data");
      res.getWriter().write(gson.toJson(responseMsg));
      return;
    }

    res.setStatus(HttpServletResponse.SC_CREATED);
    responseMsg.setMessage("POST Request Processed Successfully!");
    res.getWriter().write(gson.toJson(responseMsg));
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

}
