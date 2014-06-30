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

public class SolveMoves {
    public static class Map extends Mapper<IntWritable, MovesWritable, IntWritable, ByteWritable> {
        /**
         * Configuration and setup that occurs before map gets called for the first time.
         *
         **/
        @Override
        public void setup(Context context) {
        }

        /**
         * The map function for the second mapreduce that you should be filling out.
         */
        @Override
        public void map(IntWritable key, MovesWritable val, Context context) throws IOException, InterruptedException {
            /* YOUR CODE HERE */
	    for (Integer parent: val.getMoves()) {
		context.write(new IntWritable(parent), new ByteWritable(val.getValue()));
	    }
        }
    }

    public static class Reduce extends Reducer<IntWritable, ByteWritable, IntWritable, MovesWritable> {

        int boardWidth;
        int boardHeight;
        int connectWin;
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
            connectWin = context.getConfiguration().getInt("connectWin", 0);
            OTurn = context.getConfiguration().getBoolean("OTurn", true);
        }

        /**
         * The reduce function for the first mapreduce that you should be filling out.
         */
        @Override
        public void reduce(IntWritable key, Iterable<ByteWritable> values, Context context) throws IOException, InterruptedException {
            /* YOUR CODE HERE */
	    
	    Iterator<ByteWritable> vals = values.iterator();
	    boolean valid = false;
	    Byte best = null;
	    Byte iter = null;
	    Byte win = 1;
	    if (!OTurn) {
		win = 2;
	    }
	    /** Load first 'real' value i.e. a finished possible moves
	     *	or a SolveMoves move, setting valid bit if encountering
	     *	an undecided possible moves value along the way. */
	    while (true) {
		iter = new Byte(vals.next().get());
		if ((iter >> 2 == 0)) {
		    valid = true;
		    if (iter << 6 != 0) {
			best = iter;
			break;
		    }
		} else {
		    best = iter;
		    break;
		}
	    }
	    /** Iterate through vals and find the best move 
	     *	via minimax. */
	    while (vals.hasNext()) {
		iter = vals.next().get();
		if (!valid && iter >> 2 == 0) {
		    valid = true;
		}
		if (iter << 6 != 0) {
		    if ((best & 3) == win) {
			if ((iter & 3) == win && (best >> 2 > iter >> 2)) {
			    best = iter;
			}
		    } else if ((best & 3) == 3) {
			if (((iter & 3) == 3 && best >> 2 < iter >> 2) ||
			    (iter & 3) == win) {
			    best = iter;
			}
		    } else {
			if ((iter & 3) != (best & 3) ||
			    best >> 2 < iter >> 2) {
			    best = iter;
			}
		    }
		}
	    }
	    if (valid) {
		// add 1 to moves to end (best + 4)
		MovesWritable val = new MovesWritable((byte) (best + 4), null);
		// generate parents
		char delete = 'O';
		if (OTurn) {
		    delete = 'X';
		}
		String board = Proj2Util.gameUnhasher(key.get(), boardWidth,
						      boardHeight);
		int[] parents = new int[boardWidth];
		int genParents = 0;
		for (int i = 0; i < boardWidth * boardHeight; i += boardHeight) {
		    int j = boardHeight - 1;
		    while (j >= 0) {
			if (board.charAt(i + j) != ' ') {
			    if (board.charAt(i + j) == delete) {
				String genBoard = board.substring(0, i + j)
				    + ' ';
				if (board.length() > i + j) {
				    genBoard += board.substring(i + j + 1);
				}
				parents[genParents] = Proj2Util.gameHasher(genBoard,
								 boardWidth,
								 boardHeight);
				genParents += 1;
			    }
			    break;
			}
			j -= 1;
		    }
		}
		val.setMoves(Arrays.copyOfRange(parents, 0, genParents));
		// emit key = key and val = movesWritable
		context.write(key, val);
	    }
	}    
    }
}
