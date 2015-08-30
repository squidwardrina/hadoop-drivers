import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * Hadoop's trip data writable.
 */
public class TripDataWritable implements Writable {
	private static final String DELIM = ":";
	private IntWritable tripID = new IntWritable();
	private DoubleWritable data = new DoubleWritable();

	public TripDataWritable() {
		super();
	}

	// Java getters:
	public Integer getTripID() {
		return tripID.get();
	}

	public Double getData() {
		return data.get();
	}

	/**
	 * Set the data using java's variables.
	 * 
	 * @param tripID
	 *            the trip id to set
	 * @param data
	 *            trip's data to set
	 */
	public void set(Integer tripID, Double data) {
		if (data == null) {
			data = 0.0; // take care of case where there's no data
		}
		this.tripID.set(tripID);
		this.data.set(data);
	}

	/**
	 * Set the integer data using java's variables.
	 * 
	 * @param tripID
	 *            the trip id to set
	 * @param data
	 *            trip's integer data to set
	 */
	public void set(Integer tripID, Integer data) {
		if (data == null) {
			data = 0; // take care of case where there's no data
		}
		this.tripID.set(tripID);
		this.data.set(data.doubleValue());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		tripID.readFields(in);
		data.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(tripID.get());
		out.writeDouble(data.get());
	}

	@Override
	public String toString() {
		return tripID.toString() + DELIM + data.toString();
	}
}