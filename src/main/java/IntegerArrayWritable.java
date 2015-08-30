import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * A hadoop's writable array of inteders.
 */
public class IntegerArrayWritable extends ArrayWritable {
	private static final String DELIM = ",";

	public IntegerArrayWritable() {
		super(IntWritable.class);
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
	 * Set the array by a collection of integers.
	 * 
	 * @param values
	 *            the values of the array.
	 */
	public void set(Collection<Integer> values) {
		// Copy the values to an array of writables
		IntWritable[] valuesArr = new IntWritable[values.size()];
		int i = 0;
		for (Integer trip : values) {
			valuesArr[i] = new IntWritable(trip);
			i++;
		}

		// Set the array as result
		super.set(valuesArr);
	}

	/**
	 * Parses a written IntegerArrayWritable to list of integers.
	 * 
	 * @param written
	 *            the array written using the toString() method
	 * @return java's list of integers
	 */
	public static List<Integer> parseFromWritten(String written) {
		List<Integer> ints = new LinkedList<Integer>();
		String[] strings = written.split(DELIM); // split to strings

		// Parse each string
		for (String str : strings) {
			try {
				ints.add(Integer.parseInt(str));
			} catch (NumberFormatException e) {
				continue;
			}
		}

		return ints;
	}
}