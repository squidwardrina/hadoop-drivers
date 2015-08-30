import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Mapper for hadoop. Gets distance of each trip.
 */
public class DistMap extends
		Mapper<NullWritable, BytesWritable, Text, TripDataWritable> {
	private static final String POINTS_DELIM = "\n";
	private static final String ENCODING = "UTF-8";

	@Override
	protected void map(
			NullWritable key,
			BytesWritable tripFile,
			Mapper<NullWritable, BytesWritable, Text, TripDataWritable>.Context context)
			throws IOException, InterruptedException {
		Path filePath = ((FileSplit) context.getInputSplit()).getPath();

		// Get the trip id & the driver id from the filename
		Text driverId = new Text(filePath.getParent().getName());
		Integer tripId = getTripId(filePath);

		// Get distance of the trip
		Double tripDist = getDestinationPoint(tripFile).getDistance();

		// Write results
		TripDataWritable tripData = new TripDataWritable();
		tripData.set(tripId, tripDist);
		context.write(driverId, tripData);
	}

	/**
	 * Get the trip's final point.
	 * 
	 * @param fileBytes
	 *            trip data file
	 * @return the final point of the trip
	 * 
	 * @throws IOException
	 */
	private TripPoint getDestinationPoint(BytesWritable fileBytes)
			throws IOException {
		String tripString = IOUtils.toString(fileBytes.getBytes(), ENCODING);
		String[] strPoints = tripString.split(POINTS_DELIM);
		TripPoint destinationPoint;
		try {
			destinationPoint = new TripPoint(strPoints[strPoints.length - 2]);
		} catch (Exception e) {
			return null;
		}
		return destinationPoint;
	}

	/**
	 * Get trip ID from the filename.
	 * 
	 * @param filePath
	 *            path to the trip file
	 * @return the trip ID
	 */
	private Integer getTripId(Path filePath) {
		String tripFileName = filePath.getName(); // get file name
		int filenameEnding = tripFileName.indexOf("."); // remove ending
		String strTripId = tripFileName.substring(0, filenameEnding);
		return Integer.parseInt(strTripId);
	}
}