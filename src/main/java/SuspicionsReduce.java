import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for hadoop. Gets suspected trips for a single behavior test.
 */
public class SuspicionsReduce extends
		Reducer<Text, TripDataWritable, Text, IntegerArrayWritable> {
	private static final int CATEGORIZING_TIMES = 7;
	private IntegerArrayWritable writableSuspTrips = new IntegerArrayWritable();

	@Override
	public void reduce(Text driverId, Iterable<TripDataWritable> data,
			Context context) throws IOException, InterruptedException {
		Map<Integer, Double> trips = getTripsData(data);

		// Get the suspected false trips
		Collection<Integer> suspectedTrips = getSuspectedTrips(trips);
		writableSuspTrips.set(suspectedTrips);

		context.write(driverId, writableSuspTrips);
	}

	/**
	 * Parses the data and creates a map of trip IDs and their data.
	 * 
	 * @param dataIter
	 *            the trips iterator from hadoop's map class
	 * @return map of trips and their data
	 */
	private Map<Integer, Double> getTripsData(
			Iterable<TripDataWritable> dataIter) {
		Map<Integer, Double> trips = new HashMap<Integer, Double>();

		// Put each trip to the map
		for (TripDataWritable tripData : dataIter) {
			trips.put(tripData.getTripID(), tripData.getData());
		}

		// Return the map with trips and their data
		return trips;
	}

	/**
	 * Decides which trips are suspected to be false.
	 * 
	 * @param trips
	 *            map of all the driver's trips and their data
	 * @return collection of suspected trips
	 */
	private Collection<Integer> getSuspectedTrips(Map<Integer, Double> trips) {
		Map<Integer, Double> category1 = new HashMap<Integer, Double>();
		Map<Integer, Double> category2 = new HashMap<Integer, Double>();
		Map<Integer, Double> category3 = new HashMap<Integer, Double>();

		// Categorize the trips to 3 weight categories
		categorize(trips, category1, category2, category3);

		// Return the entries from the suspected categories
		return getSuspectedCategories(category1, category2, category3);
	}

	/**
	 * Gets 3 categories of trips and decides which of them are suspected.
	 * 
	 * @param category1
	 *            a category of trips
	 * @param category2
	 *            a category of trips
	 * @param category3
	 *            a category of trips
	 * @return collection of suspected trips
	 */
	private Collection<Integer> getSuspectedCategories(
			Map<Integer, Double> category1, Map<Integer, Double> category2,
			Map<Integer, Double> category3) {
		final int TOO_SMALL = 2;

		// Sort categories by size.
		// Add insignificant doubles to make sure the keys are different
		Map<Double, Map<Integer, Double>> sizeCategories = new TreeMap<Double, Map<Integer, Double>>();
		sizeCategories.put(category1.size() + 0.1, category1);
		sizeCategories.put(category2.size() + 0.2, category2);
		sizeCategories.put(category3.size() + 0.3, category3);

		// Final list of suspected entries
		Collection<Integer> suspected = new ArrayList<Integer>();

		// Get the sorted sizes of the categories
		Iterator<Double> sizes = sizeCategories.keySet().iterator();
		double minSize = sizes.next();
		double midSize = sizes.next();
		double maxSize = sizes.next();

		// Add the smallest category to the list of suspects
		suspected.addAll(sizeCategories.get(minSize).keySet());

		// If the second smallest category is very small - add it too
		if (minSize + midSize < maxSize / TOO_SMALL) {
			suspected.addAll(sizeCategories.get(midSize).keySet());
		}
		return suspected;
	}

	/**
	 * Categorizes all the trips to 3 categories by their data.
	 * 
	 * @param trips
	 *            map of trips and their data
	 * @param category1
	 *            category to put trips
	 * @param category2
	 *            category to put trips
	 * @param category3
	 *            category to put trips
	 */
	private void categorize(Map<Integer, Double> trips,
			Map<Integer, Double> category1, Map<Integer, Double> category2,
			Map<Integer, Double> category3) {
		// Add 3 first elements to categories
		Object[] arrTrips = trips.keySet().toArray();
		category1.put((Integer) arrTrips[0], trips.get((Integer) arrTrips[0]));
		category2.put((Integer) arrTrips[1], trips.get((Integer) arrTrips[1]));
		category3.put((Integer) arrTrips[2], trips.get((Integer) arrTrips[2]));

		// Categorize all the entries a few times for better result
		for (int i = 0; i < CATEGORIZING_TIMES; i++) {
			// Compute categories' averages
			Double avg1 = getValuesAvg(category1);
			Double avg2 = getValuesAvg(category2);
			Double avg3 = getValuesAvg(category3);

			// Move each entry to it's closest category by average
			for (Entry<Integer, Double> entry : trips.entrySet()) {
				categorizeEntry(category1, category2, category3, avg1, avg2,
						avg3, entry);
			}
		}
	}

	/**
	 * Move the entry to the closest category by it's data.
	 * 
	 * @param category1
	 *            a category of trips
	 * @param category2
	 *            a category of trips
	 * @param category3
	 *            a category of trips
	 * @param avg1
	 *            the average data of category 1
	 * @param avg2
	 *            the average data of category 2
	 * @param avg3
	 *            the average data of category 3
	 * @param entry
	 *            the entry to categorize
	 */
	private void categorizeEntry(Map<Integer, Double> category1,
			Map<Integer, Double> category2, Map<Integer, Double> category3,
			Double avg1, Double avg2, Double avg3, Entry<Integer, Double> entry) {
		// Calculate distance from each category's average
		Double currVal = entry.getValue();

		// Remove the entry from it's prev category
		category1.remove(entry.getKey());
		category2.remove(entry.getKey());
		category3.remove(entry.getKey());

		// Make a sorted map of categories by distances from curr value
		Map<Double, Map<Integer, Double>> categoryDists = new TreeMap<Double, Map<Integer, Double>>();
		categoryDists.put(Math.abs(avg1 - currVal), category1);
		categoryDists.put(Math.abs(avg2 - currVal), category2);
		categoryDists.put(Math.abs(avg3 - currVal), category3);

		// Put the entry to category with closest distance
		Double shortestDist = categoryDists.keySet().iterator().next();
		categoryDists.get(shortestDist).put(entry.getKey(), entry.getValue());
	}

	/**
	 * Compute average of map's values.
	 * 
	 * @param map
	 *            the map to compute from
	 * @return the map's values average
	 */
	private Double getValuesAvg(Map<Integer, Double> map) {
		// Go over the values and compute the average
		Double avg = 0.0;
		for (Double entry : map.values()) {
			avg += entry;
		}
		avg /= map.size();
		return avg;
	}

}