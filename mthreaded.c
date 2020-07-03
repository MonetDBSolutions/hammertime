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


#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}


void* create_procedure (void* arg) {
	monetdbe_database mdbe = arg;

	puts("Hello World!!!!");

    monetdbe_result* result;
	monetdbe_query(mdbe, "CREATE TABLE test (x integer, y string)", NULL, NULL);
	monetdbe_query(mdbe, "INSERT INTO test VALUES (42, 'Hello, '), (58, 'World!')", NULL, NULL);
	monetdbe_query(mdbe, "SELECT MIN(x), CONCAT(MIN(y), MAX(y)) FROM test; ", &result, NULL);
	monetdbe_column* rcol;
	monetdbe_result_fetch(result, &rcol, 0);

	monetdbe_column_int32_t * col_x = (monetdbe_column_int32_t *) rcol;

	printf ("check value %d\n", (int) col_x->data[0]);

	pthread_exit(NULL);
}

int main() {
	monetdbe_database mdbe = NULL;

	if (monetdbe_open(&mdbe, NULL, NULL))
		error("Failed to open database")

	pthread_t creator;

	pthread_create(&creator, NULL, &create_procedure, mdbe);

	pthread_join(creator, NULL);

	if (monetdbe_close(mdbe))
		error("Failed to close database")

     return 0;
}
