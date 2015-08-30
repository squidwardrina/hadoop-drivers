import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Mapper for hadoop. Gets trip's averall time for every trip.
 */
public class TimeMap extends
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

		// Get trip length
		Double tripLength = getTripLength(tripFile);

		// Write the result
		TripDataWritable tripData = new TripDataWritable();
		tripData.set(tripId, tripLength);
		context.write(driverId, tripData);
	}

	/**
	 * Parse trip file and compute trip's length.
	 * 
	 * @param fileBytes
	 *            the trip file
	 * @return
	 * @throws IOException
	 */
	private Double getTripLength(BytesWritable fileBytes) throws IOException {
		String tripString = IOUtils.toString(fileBytes.getBytes(), ENCODING);

		// Trip time in seconds = number of points in the trip
		String[] tripPoints = tripString.split(POINTS_DELIM);
		return (double) tripPoints.length - 1;
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