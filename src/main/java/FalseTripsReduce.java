import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for hadoop. Creates list of driver's trips and says which ones are
 * fake.
 */
public class FalseTripsReduce extends
		Reducer<Text, TripDataArrayWritable, Text, NullWritable> {
	private static final String HEADER = "driver_trip,prob\n";
	private static final char TRIPS_DELIM = '\n';
	private static final String SOLUTION_DELIM = ",";
	private static final String DRIVER_TRIP_DELIM = "_";
	private static final String FALSE_TRIP_SIGN = "0";
	private static final String TRUE_TRIP_SIGN = "1";
	private static final int TRIPS_NO = 200;
	private static final int FALSE_MIN_WEIGHT = 3;
	private boolean isFirst = true;
	private Text result = new Text();

	@Override
	public void reduce(Text tDriverID,
			Iterable<TripDataArrayWritable> suspLists, Context context)
			throws IOException, InterruptedException {
		String driverId = tDriverID.toString();

		Map<Integer, Integer> tripsSuspicionWeights = parseSuspicionWeights(suspLists);

		// Create a list of false trips
		List<Integer> falseTrips = getFalseTrips(tripsSuspicionWeights);

		// Build the result string with all the trips
		result.set(buildResultString(driverId, falseTrips));
		context.write(result, NullWritable.get());
	}

	/**
	 * Gets list of suspected trips and returns list of false trips, considering
	 * each trip's suspicion weight.
	 * 
	 * @param suspTripsWeights
	 *            map of suspected trips and their suspicion weights
	 * @return list of false trips
	 */
	private List<Integer> getFalseTrips(Map<Integer, Integer> suspTripsWeights) {
		List<Integer> falseTrips = new LinkedList<Integer>();

		// Add all the suspected trips with high enough weight of suspicion
		for (Map.Entry<Integer, Integer> entry : suspTripsWeights.entrySet()) {
			if (entry.getValue() >= FALSE_MIN_WEIGHT) {
				falseTrips.add(entry.getKey());
			}
		}
		return falseTrips;
	}

	/**
	 * Parses the suspicion lists to map of trips and their total suspicion
	 * weights.
	 * 
	 * @param suspLists
	 *            list of suspected trips and weights
	 * @return a map of suspected trips with their suspicion weights
	 */
	private Map<Integer, Integer> parseSuspicionWeights(
			Iterable<TripDataArrayWritable> suspLists) {
		Map<Integer, Integer> tripsSuspicionWeights = new HashMap<Integer, Integer>();

		// Get all the suspected lists and create a map of weights
		Iterator<TripDataArrayWritable> listsIter = suspLists.iterator();
		while (listsIter.hasNext()) {
			// Get the current list
			List<TripDataWritable> suspList = listsIter.next().getList();

			// For each suspected trip - add it's suspicion weight to the map
			for (TripDataWritable tripData : suspList) {
				// Get the trip and it's weight
				Integer tripID = tripData.getTripID();
				int currWeight = tripData.getData().intValue();

				// If no such trip yet - it's curr weight is 0
				Integer prevWeight = tripsSuspicionWeights.get(tripID);
				if (prevWeight == null) {
					prevWeight = 0;
				}

				// Update the trip's suspicion weight
				tripsSuspicionWeights.put(tripID, prevWeight + currWeight);
			}
		}
		return tripsSuspicionWeights;
	}

	/**
	 * Builds a string of all driver's trips and their analysis results.
	 * 
	 * @param driverId
	 *            the driver id
	 * @param falseTrips
	 *            list of driver's false trips
	 * @return string to be printed to file
	 */
	private String buildResultString(String driverId, List<Integer> falseTrips) {
		StringBuilder builder = new StringBuilder();

		// Add header before the first output
		if (isFirst) {
			builder.append(HEADER);
			isFirst = false;
		}
		
		// For every trip - add decision
		for (Integer tripId = 1; tripId <= TRIPS_NO; tripId++) {
			builder.append(driverId + DRIVER_TRIP_DELIM + tripId
					+ SOLUTION_DELIM);

			// Check if it's a false trip
			if (falseTrips.contains(tripId)) {
				builder.append(FALSE_TRIP_SIGN);
			} else {
				builder.append(TRUE_TRIP_SIGN);
			}
			builder.append(TRIPS_DELIM);
		}
		builder.deleteCharAt(builder.length() - 1);
		return builder.toString();
	}
}