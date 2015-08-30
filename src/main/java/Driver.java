import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver for hadoop. Analyzes drivers' behavior and finds fake trips.
 */
public class Driver {
	private static final String OUT_FOLDER = "tempOutput";
	private static final String ALL_CHILDREN = "/*";
	private static final String SET_SEPARATOR = "mapreduce.output.textoutputformat.separator";
	private static final String TIME_PATH = CheckType.time.toString();
	private static final String ACCELR_PATH = CheckType.acceleration.toString();
	private static final String DIST_PATH = CheckType.distance.toString();
	private static final int INPUT_ARG = 0;
	private static final int OUTPUT_ARG = 1;

	/**
	 * Finds fake trips for every driver.
	 * 
	 * @param args
	 *            input path and output path
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Get input/output paths
		String input = args[INPUT_ARG] + ALL_CHILDREN;
		String outFile = args[OUTPUT_ARG];

		// Analyze the driver's behavior
		analyzeBehavior(input, AccelrMap.class, ACCELR_PATH); // acceleration
		analyzeBehavior(input, TimeMap.class, TIME_PATH); // trip times
		analyzeBehavior(input, DistMap.class, DIST_PATH); // trip distances

		pickFalseTrips(outFile); // Pick the false trips
	}

	/**
	 * Clears hadoop's unnecessary generated files and renames the result file
	 * as needed.
	 * 
	 * @param outFile
	 *            the output directory
	 */
	private static void outputToFile(String outFile) {
		File outputFolder = new File(OUT_FOLDER);
		File[] generatedFiles = outputFolder.listFiles();

		// Go over the files in the output directory
		for (File file : generatedFiles) {
			String filename = file.getName();

			// Delete all the unnecessary generated files
			if (filename.startsWith(".") || filename.startsWith("_")) {
				file.delete();
			} else {
				// Rename the result file
				file.renameTo(new File(outFile));
			}
		}
		
		// Delete the directory
		outputFolder.delete();
	}

	/**
	 * Remove all the created temporary files.
	 * 
	 * @throws IOException
	 */
	private static void removeTempFiles() throws IOException {
		FileUtils.deleteDirectory(new File(TIME_PATH));
		FileUtils.deleteDirectory(new File(ACCELR_PATH));
		FileUtils.deleteDirectory(new File(DIST_PATH));
	}

	/**
	 * Performs the map reduce behavior analyze job.
	 * 
	 * @param inPath
	 *            input path
	 * @param mapper
	 *            mapper class
	 * @param outPath
	 *            path to save the operation results
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private static void analyzeBehavior(String inPath,
			Class<? extends Mapper<?, ?, ?, ?>> mapper, String outPath)
			throws IOException, InterruptedException, ClassNotFoundException {
		// Configure new job
		Configuration conf = new Configuration();
		Job analyzerJob = new Job(conf, "Analyze driver's bhavior");
		analyzerJob.setJarByClass(Driver.class);

		// Mapper settings:
		analyzerJob.setMapperClass(mapper);
		analyzerJob.setInputFormatClass(WholeFileInputFormat.class);
		analyzerJob.setMapOutputKeyClass(Text.class);
		analyzerJob.setMapOutputValueClass(TripDataWritable.class);
		WholeFileInputFormat.addInputPath(analyzerJob, new Path(inPath));

		// Reducer settings:
		analyzerJob.setReducerClass(SuspicionsReduce.class);
		analyzerJob.setOutputKeyClass(Text.class);
		analyzerJob.setOutputValueClass(TripDataArrayWritable.class);
		FileOutputFormat.setOutputPath(analyzerJob, new Path(outPath));

		analyzerJob.waitForCompletion(true);
	}

	/**
	 * Performs the job which picks the false trips.
	 * 
	 * @param outFile the file to save output into
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private static void pickFalseTrips(String outFile) throws IOException,
			InterruptedException, ClassNotFoundException {
		// Configure new job
		Configuration conf = new Configuration();
		conf.set(SET_SEPARATOR, "");
		Job sJob = new Job(conf, "Get false trips");
		sJob.setJarByClass(Driver.class);

		// Mapper settings:
		sJob.setMapperClass(FalseTripsMap.class);
		sJob.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPaths(sJob, ACCELR_PATH + "," + TIME_PATH + ","
				+ DIST_PATH);
		sJob.setMapOutputValueClass(TripDataArrayWritable.class);

		// Reducer settings:
		sJob.setReducerClass(FalseTripsReduce.class);
		sJob.setOutputKeyClass(Text.class);
		sJob.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(sJob, new Path(OUT_FOLDER));

		sJob.waitForCompletion(true);

		removeTempFiles(); // Clear the program's doodles

		outputToFile(outFile); // get the output to spec. file
	}
}
