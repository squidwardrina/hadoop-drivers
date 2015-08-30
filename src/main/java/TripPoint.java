/**
 * A point of the trip.
 */
public class TripPoint {
	private static final String DELIM = ",";
	private Double x;
	private Double y;

	/**
	 * Construct a trip point from unparsed string read from the trip file.
	 * 
	 * @param strUnparsedPoint
	 *            the string to be parsed
	 * @throws Exception
	 */
	public TripPoint(String strUnparsedPoint) throws Exception {
		// Get the info from string format "x,y"
		String[] data = strUnparsedPoint.split(DELIM);
		x = Double.parseDouble(data[0]);
		y = Double.parseDouble(data[1]);
	}

	// getters:
	public Double getX() {
		return x;
	}

	public Double getY() {
		return y;
	}

	/**
	 * Get distance of the point (from point (0, 0)).
	 * 
	 * @return the point's distance
	 */
	public Double getDistance() {
		return Math.sqrt(x * x + y * y);
	}

	/**
	 * Get the point's distance from anither point.
	 * 
	 * @param otherPoint
	 *            the other point
	 * @return distance from the given point
	 */
	public Double getDistFrom(TripPoint otherPoint) {
		double dx = x - otherPoint.getX();
		double dy = y - otherPoint.getY();
		return Math.sqrt(dx * dx + dy * dy);
	}
}
