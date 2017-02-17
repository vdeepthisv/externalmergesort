CC = gcc
FILES = external_merge.c
OUT_EXE = program

build: $(FILES)
	$(CC) -o $(OUT_EXE) $(FILES)

#clean:
    #rm -f *.o core

rebuild: clean build