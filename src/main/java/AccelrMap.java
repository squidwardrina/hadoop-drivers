import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Mapper for hadoop. Gets average acceleration for every trip.
 */
public class AccelrMap extends
		Mapper<NullWritable, BytesWritable, Text, TripDataWritable> {
	private static final String ENCODING = "UTF-8";
	private static final String POINTS_DELIM = "\n";
	private static final double STOP_VELOCITY = 0.2;
	private static final int MIN_ACCELR_TIME = 3;

	@Override
	protected void map(
			NullWritable key,
			BytesWritable fileBytes,
			Mapper<NullWritable, BytesWritable, Text, TripDataWritable>.Context context)
			throws IOException, InterruptedException {
		Path filePath = ((FileSplit) context.getInputSplit()).getPath();

		// Get the trip id & the driver id from the filename
		Text driverId = new Text(filePath.getParent().getName());
		Integer tripId = getTripId(filePath);

		// Compute the average acceleration in the trip
		Double avg = getAvgAcceleration(fileBytes);

		// Write the avg acceleration by trip id
		TripDataWritable tripData = new TripDataWritable();
		tripData.set(tripId, avg);
		context.write(driverId, tripData);
	}

	/**
	 * Analyzes the trip and gets average acceleration in it.
	 * 
	 * @param fileBytes
	 *            the trip file
	 * @return the avg acceleration in the trip
	 * @throws IOException
	 */
	private Double getAvgAcceleration(BytesWritable fileBytes)
			throws IOException {
		List<TripPoint> trip = parseTrip(fileBytes); // get the trip's points
		List<Double> vels = getVelocities(trip); // get all velocities in trip
		List<Double> accelrs = getAccelerations(vels); // get trip accelerations
		return avg(accelrs);// compute average acceleration
	}

	/**
	 * Get the trip ID from the filename.
	 * 
	 * @param filePath
	 *            path to the file
	 * @return trip ID
	 */
	private int getTripId(Path filePath) {
		String tripFileName = filePath.getName(); // get file name
		int filenameEnding = tripFileName.indexOf("."); // remove ending
		String strTripId = tripFileName.substring(0, filenameEnding);
		return Integer.parseInt(strTripId);
	}

	/**
	 * Computes average of list on numbers.
	 * 
	 * @param nums
	 *            numbers to compute avg
	 * @return the list avg
	 */
	private Double avg(List<Double> nums) {
		// If no acceleration found - return 0
		if (nums.isEmpty()) {
			return 0.0;
		}

		// Summarize the numbers
		Double sum = 0.0;
		for (Double num : nums) {
			sum += num;
		}
		return sum / nums.size(); // return the average
	}

	/**
	 * Get a list of accelerations in the trip.
	 * 
	 * @param velocities
	 *            each second's velocities list.
	 * @return the list of accelerations
	 */
	private List<Double> getAccelerations(List<Double> velocities) {
		List<Double> accelerations = new ArrayList<Double>();
		Iterator<Double> iter = velocities.iterator();
		while (iter.hasNext()) {
			double currV = iter.next();

			// If the driver stopped - get the acceleration
			if (currV < STOP_VELOCITY && iter.hasNext()) {
				double initV = currV;

				// Loop while the driver slows down
				Double nextV = iter.next();
				while (iter.hasNext() && (nextV <= currV)) {
					currV = nextV;
					initV = currV;
					nextV = iter.next();
				}

				// Loop on velocities while accelerating
				int dt = 0;
				while (iter.hasNext() && (nextV > currV)) {
					dt++;
					currV = nextV;
					nextV = iter.next();
				}

				// If accelerated at least MIN_ACCELR_SECS, count it
				if (dt >= MIN_ACCELR_TIME) {
					double dV = currV - initV;
					double acceleration = dV / dt;
					accelerations.add(acceleration);
				}
			}
		}
		return accelerations;
	}

	/**
	 * Gets a list of trip's velocities in each second.
	 * 
	 * @param trip
	 *            list of trip's points
	 * @return list of velocities
	 */
	private List<Double> getVelocities(List<TripPoint> trip) {
		List<Double> velocities = new ArrayList<Double>();
		if (trip.isEmpty()) {
			return velocities;
		}

		Iterator<TripPoint> iter = trip.iterator();
		TripPoint lastPoint;

		// Get velocity between each 2 points
		TripPoint currPoint = iter.next();
		while (iter.hasNext()) {
			lastPoint = currPoint;
			currPoint = iter.next();

			// Velocity = distance/1sec = distance
			velocities.add(lastPoint.getDistFrom(currPoint));
		}
		return velocities;
	}

	/**
	 * Parse a trip's file to list of trip points.
	 * 
	 * @param tripFile
	 *            file with the trip
	 * @return list of trip's points
	 * 
	 * @throws IOException
	 */
	private List<TripPoint> parseTrip(BytesWritable tripFile)
			throws IOException {
		// Get trip points
		String tripString = IOUtils.toString(tripFile.getBytes(), ENCODING);
		String[] strPoints = tripString.split(POINTS_DELIM);

		// Create a trip - array of points
		List<TripPoint> trip = new ArrayList<TripPoint>();
		for (String strPoint : strPoints) {
			TripPoint newPoint = null;

			// Try to create a point from the string
			boolean isPoint = true;
			try {
				newPoint = new TripPoint(strPoint);
			} catch (Exception e) {
				isPoint = false;
			}

			// If point was created - add it to the trip
			if (isPoint) {
				trip.add(newPoint);
			}
		}

		return trip;
	}
}