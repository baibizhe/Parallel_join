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

#include <assert.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include "join.h"
#define MIN(a,b) (((a)<(b))?(a):(b))
struct node{
	int nsid;
	struct node* next;
	bool occup;
}node;

int join_nested(const student_record *students, int students_count, const ta_record *tas, int tas_count)
{
	assert(students != NULL);
	assert(tas != NULL);
    if(students_count == 0 || tas_count == 0){
        return 0;
    }
	int res = 0;
    for(int i = 0; i < students_count; i++){
        if(students[i].gpa > 3){
            for(int j = 0; j < tas_count; j++){
                if(tas[j].sid == students[i].sid){
                    res ++;
                }
            }
        }
    }
	return res;
}

// Assumes that records in both tables are already sorted by sid
int join_merge(const student_record *students, int students_count, const ta_record *tas, int tas_count)
{
	assert(students != NULL);
	assert(tas != NULL);
    if(students_count == 0 || tas_count == 0){
        return 0;
    }
	int s,t;
	s=t=0;
	int res = 0;
	while(s < students_count && t < tas_count){
		if(students[s].sid > tas[t].sid){
			t++;
		}
		else if(students[s].sid < tas[t].sid){
			s++;
		}
		else{
            if(students[s].gpa > 3){
                res++;
            }
			int s1 = s+1;
			int t1 = t+1;
			while(s1 < students_count && students[s1].sid == tas[t].sid){
				if(students[s1].gpa>3) res++;
				s1++;
			}
			while(t1 < tas_count && students[s].sid == tas[t1].sid){
				if(students[s].gpa>3) res++;
				t1++;
			}
			s=s1;
			t=t1;
		}
	}
	return res;
}

int hashfunction(int sid, int hashsize){
	return sid%hashsize;
}

void node_init(struct node* n){
    n->occup = false;
    n->next = NULL;
    n->nsid = 0;
}

int join_hash(const student_record *students, int students_count, const ta_record *tas, int tas_count)
{
	assert(students != NULL);
	assert(tas != NULL);
    if(students_count == 0 || tas_count == 0){
        return 0;
    }
	int result = 0;
	int hashsize = MIN(students_count, tas_count);
	//hash initialize
	struct node *hashTable = (struct node*)malloc(sizeof(struct node)*hashsize);
	if(hashTable == NULL){
		perror("malloc hashtable\n");
		return -1;
	}
	else{
		for(int i = 0; i < hashsize; i++){
	            node_init(&hashTable[i]);
		}
	}
    int small_count = MIN(students_count, tas_count);
    bool student_less = students_count < tas_count;
	for(int i = 0; i < small_count; i++){
        int key = -1;
        int id;
        if(student_less){
            //put students in hashtable
            if(students[i].gpa > 3){
                key = hashfunction(students[i].sid, hashsize);
                id = students[i].sid;
            }
        }
        else{
            //put tas in hashtable
            key = hashfunction(tas[i].sid, hashsize);
            id = tas[i].sid;
        }
        if(key >= 0){
            struct node* ptr = &hashTable[key];
            while(ptr->next!= NULL){
                ptr = ptr->next;
            }
            if(ptr->occup == false){
                ptr->nsid = id;
                ptr->occup = true;
            }
            else{
                ptr->next = (struct node*)malloc(sizeof(struct node*));
                if(ptr->next ==NULL){
                    perror("malloc\n");
                    return -1;
                }
                ptr=ptr->next;
                ptr->nsid = id;
                ptr->occup = true;
                ptr->next = NULL;
            }
        }
		
	}
    
    if(student_less){
        //loop array of tas
        for(int i = 0; i < tas_count; i++){
            int s = tas[i].sid;
            int key = hashfunction(s, hashsize);
            struct node* ptr = &hashTable[key];
            while(ptr!= NULL){
                if(ptr->nsid == s){
                    result++;
                }
                ptr = ptr->next;
            }
        }
    }
    else{
        for(int i = 0; i < students_count; i++){
            if(students[i].gpa > 3){
                int s = students[i].sid;
                int key = hashfunction(s, hashsize);
                struct node* ptr = &hashTable[key];
                while(ptr!= NULL){
                    if(ptr->nsid == s){
                        result++;
                    }
                    ptr = ptr->next;
                }
            }
        }
    }
    
    //clean up
    for(int i = 0; i < hashsize; i++){
        struct node* ptr = &hashTable[i];
        struct node* ptrn = ptr->next;
        while(ptrn!= NULL){
            ptr = ptrn->next;
            free(ptrn);
            ptrn = ptr;
        }
    }
    free(hashTable);
	return result;
}
