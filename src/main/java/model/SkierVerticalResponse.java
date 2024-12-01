package model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SkierVerticalResponse {
  private List<SeasonVertical> resorts;
  @Data
  @AllArgsConstructor
  public static class SeasonVertical {
    private String seasonID;
    private Integer totalVert;

  }

}

