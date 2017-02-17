The algorithm takes  an input file and parameters N,B

Reading the input file (around 8 MB ) , splitting into N/b runs each of
size at most B/
As each run is created it is sorted in internal memory using quick sort
before writing to the disk.

The N/B runs are stored in a queue.

To merge B-1 runs a single record is read from each of the B-1 runs are
inserted into a priority queue.
The smallest item is then removed from the priority queue and saved ti
a temporary output file.

A new record is then read from the where the smallest item originally
came from and is inserted into the queue.

This process is repeated till all the items from K runs are read and
saved Into the temporary file in sorted order. There merge stage is
repeated hierarchically in multiple passes.
The sorted records are the output to the output file.
