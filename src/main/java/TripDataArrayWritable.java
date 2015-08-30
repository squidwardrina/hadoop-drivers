import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 * Hadoop's writable array of trips' data.
 */
public class TripDataArrayWritable extends ArrayWritable {
	private static final String DELIM = ",";

	public TripDataArrayWritable() {
		super(TripDataWritable.class);
	}

	/**
	 * Construct the array using a list of tripDatas.
	 * 
	 * @param values
	 *            list of tripDatas
	 */
	public TripDataArrayWritable(List<TripDataWritable> values) {
		super(TripDataWritable.class);
		TripDataWritable[] valuesArr = new TripDataWritable[values.size()];

		// Cast each object
		for (int i = 0; i < valuesArr.length; i++) {
			valuesArr[i] = (TripDataWritable) values.get(i);
		}

		// Set the array as result
		super.set(valuesArr);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		// Build a string of elements separated by DELIM
		for (String element : super.toStrings()) {
			builder.append(element).append(DELIM);
		}

		// Remove the last delimiter
		builder.deleteCharAt(builder.length() - 1);
		return builder.toString();
	}

	/**
	 * Create a java's list of TripDataWritables.
	 * 
	 * @return a java's list of TripDataWritables.
	 */
	public List<TripDataWritable> getList() {
		Writable[] writables = super.get();
		List<TripDataWritable> tripsData = new LinkedList<TripDataWritable>();

		// Cast each writable
		for (Writable writable : writables) {
			tripsData.add((TripDataWritable) writable);
		}

		return tripsData;
	}
}