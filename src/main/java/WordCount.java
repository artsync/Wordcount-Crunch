
import java.io.Serializable;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;

public class WordCount implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private static final String INPUT_FILE = "input.txt" ;
	private static final String OUTPUT_FILE = "output.txt";

	public static void main(final String[] args) {
		final WordCount wordCount = new WordCount();
		wordCount.count(INPUT_FILE, OUTPUT_FILE);
	}
	 
	
	private void count(final String inputFile, final String outputFile) {
		// Create an object to coordinate pipeline creation and execution.
		final Pipeline pipeline = new MRPipeline(WordCount.class);
		
		// Reference a given text file as a collection of Strings.
		final PCollection<String> lines = pipeline.readTextFile(inputFile);

		// Define a function that splits each line in a PCollection of Strings into
		// a PCollection made up of the individual words in the file.
		final PCollection<String> words = lines.parallelDo(
				new DoFn<String, String>() {
					public void process(final String line, final Emitter<String> emitter) {
						for (final String word : line.split("\\s+")) {
							emitter.emit(word);
						}
					}
				}, Writables.strings()); // Indicates the serialization format

		// The count method applies a series of Crunch primitives and returns
		// a map of the unique words in the input PCollection to their counts.
		// Best of all, the count() function doesn't need to know anything about
		// the kind of data stored in the input PCollection.
		final PTable<String, Long> counts = words.count();

		// Instruct the pipeline to write the resulting counts to a text file.
		pipeline.writeTextFile(counts, outputFile);
		
		// Execute the pipeline as a MapReduce.
		pipeline.done();
	}
}