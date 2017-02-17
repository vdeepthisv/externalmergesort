
/*
Database2 Project : Implementation of external sort merge algorithm
File: External merge sort algoithm
Name: Vijaya Deepthi Srivoleti
ID : 01542738
email : VijayaDeepthi_Srivoleti@student.uml.edu
*/

//header files
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/time.h>
#include <math.h>

// assumpptions
#define PAGE_SIZE 4100
#define BUFFER_SIZE PAGE_SIZE/ sizeof(long long int) //512 ---4096 /(sizeof long long int)
#define TEMP_FILE "tempfile.txt" //temporary file which stores the sorted data
#define LENGTH_OF_RECORD 16 // length of each record is 16 in input file
#define MB_CONV 1024 // used for conversion from MB to bytes

int pass_count=0;

struct page_data {
	
	int number_of_records; // Total number of integers to be sorted.
	int recordsperblock; // records present in one blocks while sorting.
	int records_end_block; // records present in the last block ( would not be completely divisble by records / block)
	int Number_of_blocks; // number of blocks
	int pass_num;
	char *writedata; // writing data to file
	int order; // 0 - ascending 1- descending
	int memory_limit; // memory limit provided by the user
	int offset;
};

struct blocks { 

	int buffer_current_records; // records currently present in the buffer
	long long int next_value; // get next record will be present in  file.
	long long int *buffer_array; // buffer array to store array of integers for merging
	int buffer_total_records; // total records in buffer
	long long int size_of_buffer;
	long long int buffer_index; // index for records in buffer
	};


// Swap function used for quicksort
void swap(long long int *arr , int x, int y) {
	long long int temp;
	temp = arr[x];
	arr[x] = arr[y];
	arr[y] = temp;
}

// randomised quicksort function used to sort the records in each block before writing to temp file
void rand_quicksort(long long int *arr, int first, int last, int order) {
    int pivot,j,temp,i;
    struct timeval tv;
    gettimeofday(&tv, NULL);

     if(first<last){
     	int rand_index = ((int)tv.tv_usec % (last-first+1)) + first;	// choosing pivot
		swap(arr, first, rand_index);
         pivot=first;
         i=first;
         j=last;

        while(i < j) {
         	if(order == 0) {
             while(arr[i] <= arr[pivot] && i<last)
                 i++;

             while(arr[j] > arr[pivot])
                 j--;
         	} else {
         		while(arr[i] >= arr[pivot] && i<last)
                 	i++;

	            while(arr[j] < arr[pivot])
	                j--;
         	}

             if(i<j){
                 swap(arr, i, j);
             }
        }

        swap(arr, pivot, j);
        rand_quicksort(arr,first,j-1, order); // recursive function call to the left of pivot
        rand_quicksort(arr,j+1,last, order); // recursive function call to the right of pivot
	}
}

long long int get_next_record(long long int old, int next) { // incrementing the pointer and picking the next set of integers
	return old + next*(LENGTH_OF_RECORD + 1);
}

// swap used in heap function
void heap_swap(struct blocks **arr, int x, int y) {
	struct blocks * temp = arr[x];
	arr[x] = arr[y];
	arr[y] = temp;
}


// Function to write integers into the buffer
int init_buffer(struct blocks *blocks, FILE *inputfile) {
	int count = 0;
	fseek(inputfile, blocks->next_value, SEEK_SET);
	while(blocks->buffer_current_records > 0) {
		fscanf(inputfile, "%lld", &blocks->buffer_array[count++]);
		--(blocks->buffer_current_records);
		if(count == blocks->size_of_buffer) {
			break;
		}
	}
	blocks->buffer_index = 0;
	blocks->size_of_buffer = count;
	blocks->next_value = get_next_record(blocks->next_value, count);
	return count != 0;
}


void buildheap(struct blocks **arr, int n, int i, int order)
{

    int left = (2*i) + 1;  // left = 2*i + 1
    int right = (2*i)+ 2;  // right = 2*i + 2
    int big = i;  // Initialize big as root
 
   
    if(order == 0) {
	    if (left < n && arr[left]->buffer_array[arr[left]->buffer_index] < arr[big]->buffer_array[arr[big]->buffer_index]) // left greater than root
	        big = left;
	 
	   
	    if (right < n && arr[right]->buffer_array[arr[right]->buffer_index] < arr[big]->buffer_array[arr[big]->buffer_index]) // right greater than big
	        big = right;
	} else {
	    if (left < n && arr[left]->buffer_array[arr[left]->buffer_index] > arr[big]->buffer_array[arr[big]->buffer_index])
	        big = left;
	 
	   
	    if (right < n && arr[right]->buffer_array[arr[right]->buffer_index] > arr[big]->buffer_array[arr[big]->buffer_index]) // right greater than big
	        big = right;
	}
 
    // If big is not root
    if (big != i)
    {
        heap_swap(arr, i, big);
	 buildheap(arr, n, big, order);
    }
}
 

void priorityheap(struct blocks **arr, int n, int order)	
{
		    
	int i;
    for (i = n/2 - 1; i >= 0; i--) 
        buildheap(arr, n, i, order); // build heap
}



int convertoint(char *input) { // to read the 4th argument from user : memory limit
	int n = 0, i =0;

	while(input[i] != '\0') {
		n = n*10 + input[i] - 48;
		++i;
	}

	return n;
}



// merging the sorted blocks from temporary file to output file
void mergedata(struct page_data * page_data) {
pass_count++;
	int Number_of_blocks = page_data->Number_of_blocks;
	int loaded, i;

	// Input file will be temporary file that contains sorted chunks.
	FILE *temp_file = fopen(TEMP_FILE, "r");
	FILE *outputfile;


	if(strcmp(page_data->writedata, TEMP_FILE) == 0) { // comparing the temporary file and the output file
		outputfile = fopen(page_data->writedata, "a"); // not the final pass: we append the temp file
	} else {
		outputfile = fopen(page_data->writedata, "w+"); // if final pass, we write output to the file given by the user
	}

	// Creating array of buffers and an output buffer for external merge sort.
	struct blocks ** arr = (struct blocks **) malloc(sizeof(struct blocks *)* Number_of_blocks);
	struct blocks * output_buffer = (struct blocks *) malloc(sizeof(struct blocks));

	output_buffer->size_of_buffer = BUFFER_SIZE;
	output_buffer->buffer_array = (long long int *)malloc(sizeof(long long int)*BUFFER_SIZE);
	output_buffer->buffer_index = 0;

	// initialising values for buffer
	for(i=0; i<(Number_of_blocks-1); ++i) {
		arr[i] = (struct blocks *)malloc(sizeof(struct blocks));
		arr[i]->buffer_current_records = page_data->recordsperblock; //  for 8mb : value: (1048576 * 7) = 7340032
		//printf("Recordsperblock:%d\n", page_data->recordsperblock);
		arr[i]->size_of_buffer = BUFFER_SIZE;//512
		arr[i]->buffer_array = (long long int *)malloc(sizeof(long long int)*BUFFER_SIZE);
		arr[i]->buffer_total_records = i;
		arr[i]->next_value = page_data->offset + (i * page_data->recordsperblock * (LENGTH_OF_RECORD+1)) - 1;
		if(arr[i]->next_value < 0) arr[i]->next_value = 0;

		init_buffer(arr[i], temp_file);
	}

	// Initialising buffer for last block ( will have different number of records)
	arr[i] = (struct blocks *)malloc(sizeof(struct blocks));
	arr[i]->size_of_buffer = BUFFER_SIZE;
	arr[i]->buffer_current_records = page_data->records_end_block; // for 8 mb : records in last block : 159968 
																			// 159968  + 7340032 = 7500000 => total records
	arr[i]->buffer_array = (long long int *)malloc(sizeof(long long int)*BUFFER_SIZE);
	arr[i]->buffer_total_records = i;
	//printf("buffer total records: %d\n", arr[i]->buffer_total_records ); // for 8 MB : 7
	arr[i]->next_value = page_data->offset + (i * page_data->recordsperblock * (LENGTH_OF_RECORD+1)) - 1;
	if(arr[i]->next_value < 0) arr[i]->next_value = 0;

	init_buffer(arr[i], temp_file);

	priorityheap(arr, Number_of_blocks, page_data->order);

	// Merging
	while(Number_of_blocks > 0) {
		output_buffer->buffer_array[output_buffer->buffer_index++] = arr[0]->buffer_array[arr[0]->buffer_index]; // the smallest records added to output buffer
	
		if(output_buffer->buffer_index == output_buffer->size_of_buffer) {
			for(i=0; i<output_buffer->buffer_index; ++i) {
				fprintf(outputfile, "%016lld\n", output_buffer->buffer_array[i]); // once output buffer is full, write to argv[2] output file
			}
			output_buffer->buffer_index = 0;	// set the buffer index
		}
		
		arr[0]->buffer_index++; // incrementing the buffer index to read the next small record

		// If 0th buffer is empty then load it again.
		if(arr[0]->buffer_index == arr[0]->size_of_buffer) {
			loaded = init_buffer(arr[0], temp_file);
			
			if(loaded) {
				buildheap(arr, Number_of_blocks, 0, page_data->order);	// if all the records are already loaded and buffer is empty
				//printf("no of blocks: \n%d",Number_of_blocks); : 8
			} else {	
				free(arr[0]->buffer_array);							// clearing the buffer
				free(arr[0]);
				arr[0] = arr[Number_of_blocks-1];	
				Number_of_blocks -= 1;
				buildheap(arr, Number_of_blocks, 0, page_data->order);
			}
		} else {
			buildheap(arr, Number_of_blocks, 0, page_data->order);
		}
	}


	for(i=0; i<output_buffer->buffer_index; ++i) {
		fprintf(outputfile, "%016lld\n", output_buffer->buffer_array[i]);	// writing to output file
	}

	free(output_buffer->buffer_array);
	free(output_buffer);

	fclose(temp_file);
	fclose(outputfile);
}


int external_merge_sort(struct page_data * page_data) { // handling the number of passes

	int max_blocksize = (MB_CONV*MB_CONV*page_data->memory_limit)/PAGE_SIZE - 1; //block size in bytes

	int j;

	
	// using external merge sort in specified memory limit.
	if(max_blocksize < page_data->Number_of_blocks) {
		int n = page_data->Number_of_blocks/max_blocksize;
		if(page_data->Number_of_blocks%max_blocksize)
					 n++;


		struct page_data * merge_data = (struct page_data *)malloc(sizeof(struct page_data));
		merge_data->number_of_records = page_data->number_of_records;
		merge_data->Number_of_blocks = max_blocksize;
		merge_data->recordsperblock = page_data->recordsperblock;
		merge_data->records_end_block = page_data->recordsperblock;
		merge_data->writedata = TEMP_FILE;
		merge_data->order = page_data->order;
		merge_data->memory_limit = page_data->memory_limit;
		merge_data->offset = 0;

		//printf("num of blocks: %d", merge_data->Number_of_blocks);

		//  merging blocks by calling the mergedata function
		int i;
		for(i=0; i<n; ++i) {
			if(i == n-1) {
				printf("%d Records in the last block : %d\n", page_data->recordsperblock, page_data->records_end_block);
				if(page_data->Number_of_blocks%max_blocksize) {
					merge_data->Number_of_blocks = page_data->Number_of_blocks%max_blocksize;
					printf("Number of blocks: %d", merge_data->Number_of_blocks);
				}
				merge_data->records_end_block = page_data->records_end_block;
			}
			mergedata(merge_data);
			/// passing offset as 250 * PAGE_SIZE * each_line_length(16 + 1)
			merge_data->offset = get_next_record(merge_data->offset, merge_data->recordsperblock * max_blocksize);
		}

		// Offset will be after total number of integers in input file.
		merge_data->offset = get_next_record(0, merge_data->number_of_records);
		merge_data->recordsperblock = page_data->recordsperblock * max_blocksize;
		merge_data->records_end_block = (merge_data->Number_of_blocks-1) * page_data->recordsperblock + page_data->records_end_block;
		merge_data->writedata = page_data->writedata;
		merge_data->Number_of_blocks = n;
		printf("Number of blocks:%d\n", merge_data->Number_of_blocks);
		mergedata(merge_data);
		return 2;
	} else {
		// If number of blocks is less than max_blocksize then only once external merge sort is required.
		mergedata(page_data);
		//pass_count++;

		return 1;
	}
	printf("\n maximum block size : %d\n",max_blocksize);
	//printf("\nPass count: %d",pass_count);
}

int main(int argc, char **argv) {

	struct page_data *page_data = (struct page_data *)malloc(sizeof(struct page_data));
	page_data->Number_of_blocks = 0;	
	page_data->number_of_records = 0;
	page_data->records_end_block = 0;
	
	long long int *recordsperblock = NULL;
	FILE * inputfile;
	FILE *tempfile;
	
	printf("\n*************** External sort merge **************\n");
	//printf("\n BUFFER_SIZE\n: %d",BUFFER_SIZE);
	int i;
	int cur, runs;
	int blocksize;
	struct timeval tv;
	float time_quick = 0;
	float time_external = 0;
	long long int temp_time;

	

	if(argc == 5) {
		inputfile = fopen(argv[1], "r");
		tempfile = fopen(TEMP_FILE, "w+");

		if(!inputfile) {
		printf("Error in opening input file. Please check again"); // unable to open file format
		return 1;
	}
		
		page_data->memory_limit = convertoint(argv[4]); //  otherwise assignment would make a integer from pointter without cast -wintconversion ()
		//page_data->memory_limit = argv[4]; 
		
		if(argv[3][0] == '0') {
			page_data->order = 0;	// ascending
		} else page_data->order = 1;	// descendoing

		page_data->writedata = argv[2]; // output file

	} else {
		printf("The program expects 4 arguments: inputfile, outputfile, order and memory limit.\n");
		return 1;
	}

	

	// Calculating length of a blocks for single quicksort so that process memory does not exceed limit.
	page_data->recordsperblock = (MB_CONV * MB_CONV * page_data->memory_limit)/sizeof(long long int);
	//page_data->recordsperblock = 15000; then no.of passes will be 2 ( if page size is 1024)

	recordsperblock = (long long int *) malloc(sizeof(long long int)* page_data->recordsperblock);
	
	// Getting time of day.
	gettimeofday(&tv, NULL);
	temp_time = tv.tv_sec*1000 + tv.tv_usec/1000;

	// Reading blocks from input file and then sorting it using quicksort and printing to temp file.
	printf("Number of records in each run for quicksort:");
	while(1) {
		cur = 0;
		if(feof(inputfile)) {
			break;
		}
		while( cur < page_data->recordsperblock) {
			recordsperblock[cur] = -1;
			fscanf(inputfile, "%lld", &recordsperblock[cur]);
			if(recordsperblock[cur] == -1) {
				break;
			}
			++cur;
			++page_data->number_of_records;
		}

		if(cur > 0 && cur != page_data->recordsperblock) {
			page_data->records_end_block = cur;
		}
		if(cur > 0)
			++page_data->Number_of_blocks;
		rand_quicksort(recordsperblock, 0, cur-1, page_data->order);
		//printf("%d\t",page_data->recordsperblock);
		


		for(runs = 0; runs < cur; ++runs) {
			fprintf(tempfile, "%016lld\n", recordsperblock[runs]);
		}

			printf("%d\t",runs);

		//printf("%d\t",page_data->recordsperblock);

	}
	//printf("%d\n",page_data->records_end_block );

	blocksize = (MB_CONV * MB_CONV  *page_data->memory_limit)/ PAGE_SIZE - 1; // converting to bytes -- (1024*1024*8)/4k
	printf("\nBlock size: %d\n",blocksize);

	fclose(inputfile);
	fclose(tempfile);
	free(recordsperblock);


	if(page_data->Number_of_blocks > (blocksize*blocksize)) { // if block size is more than square(blocksize) -- not possible with one temp file
		printf("File size is too big to be sorted using this.\n");
		return 0;
	}

	printf("\nTotal number of records in file: %d \n No. of blocks: %d blocks\n", page_data->number_of_records, page_data->Number_of_blocks);
	printf("Buffer Size: %ld\n",BUFFER_SIZE);

	gettimeofday(&tv, NULL);
	time_quick += (tv.tv_sec*1000 + tv.tv_usec/1000 - temp_time) /1000; // quick sort time

	if(page_data->records_end_block == 0) page_data->records_end_block = page_data->recordsperblock;

	gettimeofday(&tv, NULL);
	temp_time = tv.tv_sec*1000 + tv.tv_usec/1000;
	external_merge_sort(page_data);

	printf("Passes required in merge sort: %d\n", external_merge_sort(page_data));

	gettimeofday(&tv, NULL);
	time_external +=( tv.tv_sec*1000 + tv.tv_usec/1000 - temp_time)/1000;

	 //printf("\n No of passes in merge sort: %d\n", pass_count);

	printf("Time taken by quicksort function : %f secs\n",time_quick );
	printf("Time taken by external mergesort function : %f secs\n", time_external);

	return 0;
}

