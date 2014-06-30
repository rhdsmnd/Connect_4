
This is an excerpt from a class project, for complete details
read: 

http://www-inst.eecs.berkeley.edu/~cs61c/sp14/projs/02/

PossibleMoves.java uses MapReduce to generate all possible moves
(even invalid ones) of an NxN Connect 4 game.  It feeds all
its output SolveMoves.java, which filters valid moves and assigns
a unique value to each, which an AI can use to discover the best move.
