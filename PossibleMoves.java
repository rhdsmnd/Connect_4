/*
 * CS61C Spring 2014 Project2
 * Reminders:
 *
 * DO NOT SHARE CODE IN ANY WAY SHAPE OR FORM, NEITHER IN PUBLIC REPOS OR FOR DEBUGGING.
 *
 * This is one of the two files that you should be modifying and submitting for this project.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Math;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PossibleMoves {
    public static class Map extends Mapper<IntWritable, MovesWritable, IntWritable, IntWritable> {
        int boardWidth;
        int boardHeight;
        boolean OTurn;
        /**
         * Configuration and setup that occurs before map gets called for the first time.
         *
         **/
        @Override
        public void setup(Context context) {
            // load up the config vars specified in Proj2.java#main()
            boardWidth = context.getConfiguration().getInt("boardWidth", 0);
            boardHeight = context.getConfiguration().getInt("boardHeight", 0);
            OTurn = context.getConfiguration().getBoolean("OTurn", true);
        }

        /**
         * The map function for the first mapreduce that you should be filling out.
         */
        @Override
        public void map(IntWritable key, MovesWritable val, Context context) throws IOException, InterruptedException {
            if (val.getStatus() == 0) {
		String board = Proj2Util.gameUnhasher(key.get(), boardWidth, boardHeight);
		for (int i = 0; i < boardWidth * boardHeight; i += boardHeight) {
		    int j = 0;
		    while (j < boardHeight) {
			if (board.charAt(j + i) == ' ') {
			    char [] boardChar = board.toCharArray();
			    if (OTurn) {
				boardChar[j + i] = 'O';
			    } else {
				boardChar[j + i] = 'X';
			    }
			    IntWritable child =
				new IntWritable(Proj2Util.gameHasher(new String(boardChar),
								     boardWidth,
								     boardHeight));
			    context.write(child, key);
			    break;
			}
			j += 1;
		    }
		}
	    }
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, MovesWritable> {

        int boardWidth;
        int boardHeight;
        int connectWin;
        boolean OTurn;
        boolean lastRound;
        /**
         * Configuration and setup that occurs before reduce gets called for the first time.
         *
         **/
        @Override
        public void setup(Context context) {
            // load up the config vars specified in Proj2.java#main()
            boardWidth = context.getConfiguration().getInt("boardWidth", 0);
            boardHeight = context.getConfiguration().getInt("boardHeight", 0);
            connectWin = context.getConfiguration().getInt("connectWin", 0);
            OTurn = context.getConfiguration().getBoolean("OTurn", true);
            lastRound = context.getConfiguration().getBoolean("lastRound", true);
        }

        /**
         * The reduce function for the first mapreduce that you should be filling out.
         */
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            /* YOU CODE HERE */
	    int status = 0;
	    if (Proj2Util.gameFinished(Proj2Util.gameUnhasher(key.get(), boardWidth, boardHeight),
				       boardWidth, boardHeight, connectWin)) {
		if (OTurn) {
		    status = 1;
		} else {
		    status = 2;
		}
	    } else if (!(Proj2Util.gameUnhasher(key.get(),
						boardWidth, boardHeight).contains(" "))) {
		status = 3;
	    }
	    MovesWritable output = new MovesWritable(status, 0, null);
	    ArrayList<Integer> parents = new ArrayList<Integer>();
	    for (IntWritable parent: values) {
		parents.add(parent.get());
	    }
	    int[] primitiveParents = new int[parents.size()];
	    int i = 0;
	    for (Integer elem: parents) {
		primitiveParents[i] = parents.get(i);
		i += 1;
	    }
	    output.setMoves(primitiveParents);
	    context.write(key, output);
	}
    }
}
