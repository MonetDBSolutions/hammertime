/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdbe.h"
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <pthread.h>


#define error(msg) fprintf(stderr, "Failure in %s: %s\n", __func__, msg);

#define MAX_SIZE_PART_TABLE  50
#define MAX_SIZE_MERGE_TABLE 200

#include <stdatomic.h>

atomic_int latest_partition_table;

atomic_int oldest_partition_table;

char *dbpath = NULL;

void* partition_procedure (void* arg) {
	monetdbe_database mdbe = arg;

	if (monetdbe_open(&mdbe, dbpath, NULL))
		error("Failed to open database")

	monetdbe_query(mdbe, "CREATE MERGE TABLE IF NOT EXISTS mt (x INTEGER)", NULL, NULL);
	monetdbe_query(mdbe, "CREATE TABLE IF NOT EXISTS pt_0 (x INTEGER)", NULL, NULL);
	monetdbe_query(mdbe, "ALTER TABLE mt ADD TABLE pt_0", NULL, NULL);

	atomic_fetch_add(&latest_partition_table, 1);

	bool loop = true;
	while(loop) {
		char* err;
		/*
		// Clean up the previously failed transaction if present.
		if ((err = monetdbe_query(mdbe, "ROLLBACK", NULL, NULL)) != NULL) {
			error(err)
			continue;
		}
		*/

		char query_string_buffer[256];

		int part = (int) atomic_load(&latest_partition_table);


		sprintf(query_string_buffer, "SELECT COUNT(*) FROM pt_%d", part);
		printf("%s\n", query_string_buffer);
		monetdbe_result* result;
		if ((err = monetdbe_query(mdbe, query_string_buffer, &result, NULL)) != NULL) {
			error(err)
			continue;
		}
		monetdbe_column* rcol;
		monetdbe_result_fetch(result, &rcol, 0);

		monetdbe_column_int128_t * col_x = (monetdbe_column_int128_t *) rcol;

		size_t size_latest_partition = (size_t) (__int128_t) col_x->data[0];

		if ((err = monetdbe_cleanup_result(mdbe, result)) != NULL)
			error(err)

		if (size_latest_partition < MAX_SIZE_PART_TABLE) {
			continue;
		}

		int next_part = part + 1;

		sprintf(query_string_buffer, "CREATE TABLE pt_%d (x INTEGER)", next_part);
		printf("%s\n", query_string_buffer);
		if ((err = monetdbe_query(mdbe, query_string_buffer, NULL, NULL)) != NULL) {
			error(err)
			continue;
		}

		atomic_store(&latest_partition_table, next_part);

		sprintf(query_string_buffer, "ALTER TABLE mt ADD TABLE pt_%d", next_part);
		printf("%s\n", query_string_buffer);
		if ((err = monetdbe_query(mdbe, query_string_buffer, NULL, NULL)) != NULL) {
			error(err)
			continue;
		}

	}

	if (monetdbe_close(mdbe)) {
        error("Failed to close database");
    }

	pthread_exit(NULL);
}

void* insert_procedure (void* arg) {
	monetdbe_database mdbe = arg;

	if (monetdbe_open(&mdbe, dbpath, NULL))
		error("Failed to open database")

	while (  ((int) atomic_load(&latest_partition_table)) < 0)  {};

	bool loop = true;
	while(loop) {

		char query_string_buffer[256];

		int part = (int) atomic_load(&latest_partition_table);

		sprintf(query_string_buffer, "INSERT INTO pt_%d VALUES (RAND()), (RAND()), (RAND()), (RAND()), (RAND()), (RAND()), (RAND()), (RAND()), (RAND()), (RAND())", part);
		printf("%s\n", query_string_buffer);
		char* err;
		if ((err = monetdbe_query(mdbe, query_string_buffer, NULL, NULL)) != NULL)
			error(err)
	}

	if (monetdbe_close(mdbe)) {
        error("Failed to close database");
    }

	pthread_exit(NULL);
}

void* delete_procedure (void* arg) {
	monetdbe_database mdbe = arg;

	if (monetdbe_open(&mdbe, dbpath, NULL))
		error("Failed to open database")

    bool loop = true;
	while(loop) {

		if (  ! ( ((int) atomic_load(&oldest_partition_table)) < (int) atomic_load(&latest_partition_table)) )  continue;

		char* err;
		monetdbe_result* result;
		printf("SELECT COUNT(x) FROM mt\n");
		if ((err = monetdbe_query(mdbe, "SELECT COUNT(x) FROM mt", &result, NULL)) != NULL) {
			error(err)
			continue;
		}

		monetdbe_column* rcol;
		monetdbe_result_fetch(result, &rcol, 0);

		monetdbe_column_int128_t * col_x = (monetdbe_column_int128_t *) rcol;

		size_t total_size_merge_table = (__int128_t) col_x->data[0];

		if (total_size_merge_table < MAX_SIZE_MERGE_TABLE) {
			continue;
		}

		char query_string_buffer[256];
		int part = (int) atomic_load(&oldest_partition_table);
		sprintf(query_string_buffer, "DROP TABLE pt_%d CASCADE", part);
		printf("%s\n", query_string_buffer);
		if ((err = monetdbe_query(mdbe, query_string_buffer, NULL, NULL)) != NULL) {
			error(err)
			continue;
		}

		atomic_store(&oldest_partition_table, part+1);
	}

	if (monetdbe_close(mdbe)) {
        error("Failed to close database")
    }

	pthread_exit(NULL);
}

int main( int argc, char *argv[]) 
{
	if (argc != 2) {
		printf("usage: mthreaded2 dbpath\n");
		return -1;
	}
		
	dbpath = argv[1];

	printf("%s\n", dbpath);
	atomic_init(&latest_partition_table, -1);
	atomic_init(&oldest_partition_table, 0);

	monetdbe_database mdbe = NULL;

	pthread_t deleter;
	pthread_t inserter;
	pthread_t partitioner;

	pthread_create(&inserter, NULL, &insert_procedure, mdbe);
	pthread_create(&deleter, NULL, &delete_procedure, mdbe);
	pthread_create(&partitioner, NULL, &partition_procedure, mdbe);

	pthread_join(inserter, NULL);
	pthread_join(deleter, NULL);
	pthread_join(partitioner, NULL);

     return 0;
}
