// ------------

// -------------

#include <limits.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

#include "join.h"
#include "options.h"


#define MAX(x,y) (((x)>(y))?(x):(y))

#define ROOT 0
int sum(int *array,int size);
int main(int argc, char *argv[])
{
	MPI_Init(&argc, &argv);

	int result = 1;
	student_record *students = NULL;
	ta_record *tas = NULL;

	const char *path = parse_args(argc, argv);
	if (path == NULL) goto end;

	if (!opt_replicate && !opt_symmetric) {
		fprintf(stderr, "Invalid arguments: parallel algorithm (\'-r\' or \'-s\') is not specified\n");
		print_usage(argv);
		goto end;
	}

	int parts, id;
	MPI_Comm_size(MPI_COMM_WORLD, &parts);
	MPI_Comm_rank(MPI_COMM_WORLD, &id);

	// Load this process'es partition of data
	char part_path[PATH_MAX] = "";
	snprintf(part_path, sizeof(part_path), "%s_%d", path, id);
	int students_count, 
		tas_count,
		single_result;/* the count result for single process , will need to reduce to sum*/
		
	if (load_data(part_path, &students, &students_count, &tas, &tas_count) != 0) goto end;

	join_func_t *join_f = opt_nested ? join_nested : (opt_merge ? join_merge : join_hash);
	int count = -1;

	MPI_Barrier(MPI_COMM_WORLD);
	double t_start = MPI_Wtime();

	//TODO: parallel join using MPI	
	
	if(opt_replicate){
		int sum_student_count,  /* the total number of students in all distributed dataset*/
			sum_ta_count,/* the total number of tas in all distributed dataset*/
			* all_student_count, /* the array to store each student count in each dataset */
			* all_ta_count,/* the array to store each ta count in each dataset*/
			*disps /* the displacement to communicate for MPI_Allgatherv()  */
			;

		disps =(int*) malloc(sizeof(int)*parts);
		all_student_count =(int * )malloc(sizeof(int)*parts);
		all_ta_count =(int * )malloc(sizeof(int)*parts);
		disps[0] = 0; /* the displace start at 0*/
		
		MPI_Allgather(&students_count,1,MPI_INT,all_student_count,1,MPI_INT,MPI_COMM_WORLD); // get all student count from other process
		MPI_Allgather(&tas_count,1,MPI_INT,all_ta_count,1,MPI_INT,MPI_COMM_WORLD);// get all ta count from other process


		sum_student_count = sum(all_student_count,parts);
		sum_ta_count = sum(all_ta_count,parts);
		

		int recvbufsize = MAX(sum_student_count,sum_ta_count);  // decide which part to partition
		for (int i=1; i<parts; i++){
			if(sum_student_count==recvbufsize)disps[i] = disps[i-1] + all_ta_count[i-1];
			else if(sum_ta_count==recvbufsize)disps[i] = disps[i-1] + all_student_count[i-1];
			}

	
		if(recvbufsize ==sum_student_count){ 
			// this is the case the tas records are larger, we need partition  on students record, collect all ta records
			ta_record *all_tas =(ta_record *) malloc(sizeof(ta_record )*sum_ta_count);
			if(all_tas ==NULL)perror("malloc all tas error");

			MPI_Datatype person_type;
			int lengths[3] = { 1, 1, 8 };
			const MPI_Aint displacements[3] = { 0, sizeof(int), sizeof(int) + sizeof(int) }; //displacements to commit struct
			MPI_Datatype types[3] = { MPI_INT,MPI_INT, MPI_CHAR};
			MPI_Type_create_struct(3, lengths, displacements, types, &person_type);
			MPI_Type_commit(&person_type);
			MPI_Allgatherv(tas, //const void *sendbuf,
							tas_count, //int sendcount
							person_type, //MPI_Datatype sendtype,
							all_tas , // void *recvbuf
							all_ta_count, // const int recvcounts[],
							disps, //const int displs[]
							person_type, // MPI_Datatype recvtype
							MPI_COMM_WORLD //MPI_Comm comm;
							);		
			single_result =join_f(students,students_count,all_tas,sum_ta_count);
			MPI_Barrier(MPI_COMM_WORLD);			 // need to wait all process to finish
			MPI_Reduce(&single_result,&count,1,MPI_INT,MPI_SUM,0,MPI_COMM_WORLD); // collect all result to count
			free(all_tas);
			}
		else{ // this is the case that  tas record larger than students record, we need partition tas'records , and collect all students records;			
			student_record* all_students = (student_record *)malloc(sizeof(student_record)*sum_student_count);
			if (all_students==NULL)perror("malloc all students error");	

			MPI_Datatype person_type;	 // construct the struct to communicate
			int lengths[3] = { 1, 20, 1 };
			const MPI_Aint displacements[3] = { 0, sizeof(int), sizeof(int) + 20*sizeof(char) };  // the displacements inside person_type
			MPI_Datatype types[3] = { MPI_INT, MPI_CHAR, MPI_DOUBLE };
			MPI_Type_create_struct(3, lengths, displacements, types, &person_type);
			MPI_Type_commit(&person_type);

			MPI_Allgatherv(students, //const void *sendbuf,
							students_count, //int sendcount
							person_type, //MPI_Datatype sendtype,
							all_students , // void *recvbuf
							all_student_count, // const int recvcounts[],
							disps, //const int displs[]
							person_type, // MPI_Datatype recvtype
							MPI_COMM_WORLD //MPI_Comm comm);
							);
			single_result = join_f(all_students,sum_student_count,tas,tas_count);
			MPI_Barrier(MPI_COMM_WORLD);			 // need to wait all process to finish
			MPI_Reduce(&single_result,&count,1,MPI_INT,MPI_SUM,0,MPI_COMM_WORLD); // collect all result to count
			free(all_students);		
			}
		free(disps);
		free(all_student_count);
		free(all_ta_count);
		}
	else{  // this is the case symmetric patition
		int single_result = join_f(students,students_count,tas,tas_count);
        MPI_Barrier(MPI_COMM_WORLD);
		MPI_Reduce(&single_result,&count,1,MPI_INT,MPI_SUM,0,MPI_COMM_WORLD); // collect all result to count
	}
	if(id!=ROOT){
		count=1;
	}
	double t_end = MPI_Wtime();
	if (count < 0) goto end;
	if (id == 0) {
		printf("%d\n", count);
		printf("%f\n", (t_end - t_start) * 1000.0);

	}

	result = 0;

end:
	if (students != NULL) free(students);
	if (tas != NULL) free(tas);
	MPI_Finalize();
	return result;
}
int sum(int *array,int size){
	int summation=0;
	for(int i=0;i<size;i++){
		summation+=array[i];
	}
	return summation;
}
