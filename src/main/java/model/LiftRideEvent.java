package model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LiftRideEvent {
  private Integer resortID;
  private String seasonID;
  private Integer dayID;
  private Integer skierID;
  private LiftRide liftRide;

}
