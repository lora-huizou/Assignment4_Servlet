package model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LiftRideEvent {
  private int resortID;
  private String seasonID;
  private int dayID;
  private int skierID;
  private LiftRide liftRide;

}
