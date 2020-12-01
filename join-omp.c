// ------------
// This code is provided solely for the personal and private use of
// students taking the CSC367H5 course at the University of Toronto.
// Copying for purposes other than this use is expressly prohibited.
// All forms of distribution of this code, whether as given or with
// any changes, are expressly prohibited.
//
// Authors: Bogdan Simion, Alexey Khrabrov
//
// All of the files in this directory and all subdirectories are:
// Copyright (c) 2019 Bogdan Simion
// -------------

#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

#include "join.h"
#include "options.h"
#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))
int main(int argc, char *argv[])
{
	const char *path = parse_args(argc, argv);
	if (path == NULL) return 1;

	if (!opt_replicate && !opt_symmetric) {
		fprintf(stderr, "Invalid arguments: parallel algorithm (\'-r\' or \'-s\') is not specified\n");
		print_usage(argv);
		return 1;
	}

	if (opt_nthreads <= 0) {
		fprintf(stderr, "Invalid arguments: number of threads (\'-t\') not specified\n");
		print_usage(argv);
		return 1;
	}
	omp_set_num_threads(opt_nthreads);

	int students_count, tas_count;
	student_record *students;
	ta_record *tas;
	if (load_data(path, &students, &students_count, &tas, &tas_count) != 0) return 1;

	int result = 1;
	join_func_t *join_f = opt_nested ? join_nested : (opt_merge ? join_merge : join_hash);

	double t_start = omp_get_wtime();
    int count = -1;
   // (void)join_f;
	//TODO: parallel join using OpenMP
    if(opt_replicate){
        //method 1, split on smaller array
	int s_count = MAX(students_count, tas_count);
	bool s_less = false;
	if(students_count < tas_count){
		s_less = true;
	}
        int work = (s_count + opt_nthreads)/opt_nthreads;
        count = 0;
#pragma omp parallel default(none) shared(work, s_less, s_count, students_count, students, tas, tas_count, join_f, opt_nthreads) reduction(+:count)
{
    int tid = omp_get_thread_num();
	if(!s_less){
		count += join_f(students + work *tid, MIN(work, students_count - work *tid), tas, tas_count);
	}
	else{
		count += join_f(students + work *tid, MIN(work, students_count - work *tid), tas, tas_count);
	}
}
    }
    else{
        //method 2
        int work = (students_count + opt_nthreads)/opt_nthreads;
        count = 0;
#pragma omp parallel default(none) shared(work, students_count, students, tas, tas_count, join_f, opt_nthreads) reduction(+:count) num_threads(opt_nthreads)
{
	int tid = omp_get_thread_num();
    int tmin, tmax, smin, smax;
    smin = students[work*tid].sid;
    smax = students[MIN(work*tid + work, students_count-1)].sid;
    tmin = tas_count  *tid / opt_nthreads + 1;
    tmax = (tas_count * (tid+1) / opt_nthreads) - 1;

    while(tas[tmin].sid >= smin && tmin > 0) {
        tmin -= 1;
        if(tmin == 0) break;
    }
	while(tas[tmax].sid <= smax && tmax < tas_count -1){
		tmax += 1;
		if(tmax == tas_count - 1) break;
	}
        int tcount = tmax - tmin + 1;
        count += join_f(students + work*tid, MIN(students_count - work *tid, work), tas + tmin, tcount);
}
    }
	

	double t_end = omp_get_wtime();

	if (count < 0) goto end;
	printf("%d\n", count);
	printf("%f\n", (t_end - t_start) * 1000.0);
	result = 0;

end:
	free(students);
	free(tas);
	return result;
}
