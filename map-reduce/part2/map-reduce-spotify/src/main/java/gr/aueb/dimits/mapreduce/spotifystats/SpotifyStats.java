package gr.aueb.dimits.mapreduce.spotifystats;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SpotifyStats {

    /**
     * String delimiter for the intermediary output tuple (which is implemented as a string).
     */
    public static final String OUTPUT_DELIMITER = "#";


    /**
     * <p>
     * A Mapper subclass which accepts a (KEY, ROW) tuple as input and maps it to a
     * ((Country: year-month), (song_name, danceability)) intermediate format.
     * </p>
     * <p>
     * All tuples are represented with strings, with values denoted by the
     * OUTPUT_DELIMITER class variable.
     * </p>
     */
    public static class SongMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final String CSV_TOKENIZER_REGEX =",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
        private static final int NAME_IDX = 1;
        private static final int COUNTRY_IDX = 6;
        private static final int DATE_IDX = 7;
        private static final int DANCE_IDX = 13;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // we ignore the key

            // split a line into words
            String[] tokens = value.toString().split(CSV_TOKENIZER_REGEX, -1);

            String name = tokens[NAME_IDX].trim();
            // prevent probable Reducer error if OUTPUT_DELIMITER is included in the song's name
            name = name.replace(OUTPUT_DELIMITER, "");
            name = processString(name);

            String country = tokens[COUNTRY_IDX].trim();
            country = processString(country);
            if (country.trim().isEmpty())
                country = "GLOBAL";

            String date = tokens[DATE_IDX];
            date = processString(date);
            if(date.trim().isEmpty())
                return;
            String monthStr = date.substring(5, 7);
            String yearStr = date.substring(0, 4);

            // don't convert to float as value processing takes place at the Reducer
            String danceabilityStr = tokens[DANCE_IDX];

            // skip row if danceability is missing (no valuable data)
            if (danceabilityStr.trim().isEmpty())
                return;
            // remove quotation marks at start and end of string
            danceabilityStr = processString(danceabilityStr);

            // field checking
            float danceability;
            int month;
            int year;
            try{
                danceability = Float.parseFloat(danceabilityStr);
                month = Integer.parseInt(monthStr);
                year = Integer.parseInt(yearStr);
            } catch(NumberFormatException nfe){
                // if any error on math formatting (which in  large datasets is likely) ignore row
                // this is essentially a filter operation and will reduce the amount of rows that go to the reducer
                // also this will catch the header
                return;
            }

            // No framework support for tuples, therefore we create a string equivalents

            // same key for records in the same country and month pair
            String dateKey = String.format("%s%s%d%s%d", country, OUTPUT_DELIMITER, year, OUTPUT_DELIMITER,  month);
            // write name and danceability as strings
            String output = String.format("%s%s%f", name, OUTPUT_DELIMITER, danceability);

            // write output key and value tuples
            context.write(new Text(dateKey), new Text(output));
        }

        private String processString(String string){
            return string.substring(1, string.length() - 1);
        }

    }

    /**
     * A reducer subclass which takes as input string-tuples of the format documented in SongMapper.
     * Outputs are strings  of the format ((Country: year-month), (name: max_danceability, avg: avg(danceability)))
     */
    public static class SongReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String bestSong = null;
            float maxDanceability = -999999;
            float sumDanceability = 0;
            int numRecords = 0;

            for (Text value: values) {
                // unpack string tuple
                String[] valueTuple = value.toString().split(OUTPUT_DELIMITER);
                String name = valueTuple[0];

                float danceability;
                try{
                    danceability = Float.parseFloat(valueTuple[1]);
                } catch(NumberFormatException nfe){
                    throw new NumberFormatException(String.format("Key: %s\nTuple: %s\nError on: %s", key, value, valueTuple[1]));
                }

                // max calculation
                if (danceability > maxDanceability){
                    maxDanceability = danceability;
                    bestSong = name;
                }

                // avg calculation
                sumDanceability += danceability;
                numRecords++;
            }

            // unpack string tuple
            String[] keyTuple = key.toString().split(OUTPUT_DELIMITER);
            //format key
            String output_key = String.format("%s: %s-%s", keyTuple[0], keyTuple[1], keyTuple[2]);
            // format value
            String output_value = String.format("%s: %f, avg: %f",
                    bestSong, maxDanceability, (sumDanceability/numRecords));

            // write formatted output
            context.write(new Text(output_key), new Text(output_value));
        }

    }
}
