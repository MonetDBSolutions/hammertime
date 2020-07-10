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


#define error(msg) fprintf(stderr, "Failure: %s\n", msg);

void* insert_procedure (void* arg) {
	monetdbe_database mdbe = arg;

	puts("i: start open");

	if (monetdbe_open(&mdbe, "/home/aris/sources/hammertime/devdb", NULL))
		error("Failed to open database")

	puts("i: end open");

    
	monetdbe_query(mdbe, "CREATE TABLE IF NOT EXISTS test (x INTEGER)", NULL, NULL);

	bool loop = true;

	while(loop) {

		monetdbe_query(mdbe, "INSERT INTO test VALUES (RAND()), (RAND()), (RAND()), (RAND())", NULL, NULL);

		monetdbe_result* result;
		monetdbe_query(mdbe, "SELECT x FROM test; ", &result, NULL);
		monetdbe_column* rcol;
		monetdbe_result_fetch(result, &rcol, 0);

		monetdbe_column_int32_t * col_x = (monetdbe_column_int32_t *) rcol;

		for (size_t i = 0 ; i < col_x->count; i++) {
			printf ("from %s: check value %d\n",__func__, (int) col_x->data[i]);
		}
		char* err;
		if ((err = monetdbe_cleanup_result(mdbe, result)) != NULL)
			error(err)
	}

	puts("i: start close");

	if (monetdbe_close(mdbe)) {
        error("Failed to close database");
    }

	puts("i: end close");

	pthread_exit(NULL);
}

void* delete_procedure (void* arg) {
	monetdbe_database mdbe = arg;

	puts("d: start open");

	if (monetdbe_open(&mdbe, "/home/aris/sources/hammertime/devdb", NULL))
		error("Failed to open database")

	puts("d: end open");

    monetdbe_query(mdbe, "CREATE TABLE IF NOT EXISTS test (x INTEGER)", NULL, NULL);

	bool loop = true;

	while(loop) {

		monetdbe_query(mdbe, "DELETE FROM test WHERE x < (SELECT MEDIAN(x) FROM test)", NULL, NULL);
	
		monetdbe_result* result;
		monetdbe_query(mdbe, "SELECT x FROM test; ", &result, NULL);
		monetdbe_column* rcol;
		monetdbe_result_fetch(result, &rcol, 0);

		monetdbe_column_int32_t * col_x = (monetdbe_column_int32_t *) rcol;

		for (size_t i = 0 ; i < col_x->count; i++) {
			printf ("from %s: check value %d\n",__func__, (int) col_x->data[i]);
		}

		char* err;
		if ((err = monetdbe_cleanup_result(mdbe, result)) != NULL)
			error(err)
	}

	puts("d: start close");

	if (monetdbe_close(mdbe)) {
        error("Failed to close database")
    }

	puts("d: end close");

	pthread_exit(NULL);
}

int main() {
	monetdbe_database mdbe = NULL;

	pthread_t deleter;
	pthread_t inserter;

	pthread_create(&inserter, NULL, &insert_procedure, mdbe);
	pthread_create(&deleter, NULL, &delete_procedure, mdbe);

	pthread_join(inserter, NULL);

	pthread_join(deleter, NULL);
		

     return 0;
}
