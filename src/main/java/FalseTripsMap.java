import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Hadoop mapper. Receives a driver's suspected trips and adds weight for the
 * check.
 */
public class FalseTripsMap extends
		Mapper<LongWritable, Text, Text, TripDataArrayWritable> {
	private static final String ENTRY_DELIM = "\t";
	private static final int ACCLR_WEIGHT = 2;
	private static final int TIME_WEIGHT = 1;
	private static final int DIST_WEIGHT = 1;

	@Override
	protected void map(
			LongWritable key,
			Text line,
			Mapper<LongWritable, Text, Text, TripDataArrayWritable>.Context context)
			throws IOException, InterruptedException {
		// Get the current check's weight
		Integer weight = getCheckWeight((FileSplit) context.getInputSplit());

		// Get the driver ID
		String[] split = line.toString().split(ENTRY_DELIM);
		String driverId = split[0];

		// Parse suspected trips to TripData
		TripDataArrayWritable tripsData = parseTripDataArray(weight, split[1]);

		// Write result
		context.write(new Text(driverId), tripsData);
	}

	/**
	 * Parses the string of suspected trips to an array of trips and weight of
	 * check.
	 * 
	 * @param weight
	 *            the weight of current check
	 * @param unparsed
	 *            the string of trips as read from file
	 * @return writable array of suspected trips and weight
	 */
	private TripDataArrayWritable parseTripDataArray(Integer weight,
			String unparsed) {
		List<TripDataWritable> tripsData = new LinkedList<TripDataWritable>();

		// Parse trips from string
		List<Integer> trips = IntegerArrayWritable.parseFromWritten(unparsed);

		// Add weight for every trip
		TripDataWritable tripData = new TripDataWritable();
		for (Integer trip : trips) {
			tripData.set(trip, weight);
			tripsData.add(tripData);
		}

		// Return an array of writable trips
		return new TripDataArrayWritable(tripsData);
	}

	/**
	 * Get the weight of the current check from the filename containing the
	 * check's name.
	 * 
	 * @param file
	 *            the current file
	 * @return the weight of the check
	 */
	private Integer getCheckWeight(FileSplit file) {
		Path folder = file.getPath().getParent();
		CheckType checkType = CheckType.valueOf(folder.getName());

		// Create map from check type to it's weight
		Map<CheckType, Integer> weights = new HashMap<CheckType, Integer>();
		weights.put(CheckType.acceleration, ACCLR_WEIGHT);
		weights.put(CheckType.time, TIME_WEIGHT);
		weights.put(CheckType.distance, DIST_WEIGHT);

		// Get current check's weight
		return weights.get(checkType);
	}
}
