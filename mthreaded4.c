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



/* Tg (wtime)
 * // Create new transactions T1 and T2 based on global transaction Tg
 * T1 -> stime = Tg->wtime
 * T1->wstime = timestamp()
 * 
 * T2->stime = Tg->wtime
 * T2->wstime = timestamp()
 * 
 * // Since T1 called timestamp() first we can
 * assert (T1->wstime < T2->wstime)
 * 
 * // Both T1 and T2 do writes on some table t
 * t->base.wtime = T1->wstime // for the version of t in T1
 * t->base.wtime = T2->wstime // for the version of t in T2
 * 
 * // Now say that T1 commits first. I.e. it merges with the global transaction.
 * // For the version of table t in the global transaction denoted by ot we now have
 * 
 * ot->base.wtime == T1->wstime
 * 
 * // Now T2 can try to commit
 * // It hase nonzero t->base.wtime =  because it had a writes and so it will enter the following check
 * 
 * t->base.wtime < ot->base.wtime // (C1) where t is basically the version of the table after T1 and ot the version of the table after the commit of T2
 * 
 * // But because t->base.wtime = T2-wstime and  ot->base.wtime T1->wstime
 * 
 * // C1 will evaluate assert
 * 
 * T2->wstime < T1->wstime // this is false.
 * 
 * This causes T2 to overwrite a table that has change unknown to T2 corrupting the transactional validity of the table.
 */

#define error(msg) fprintf(stderr, "Failure in %s: %s\n", __func__, msg);

void get_content_of_table(monetdbe_database mdbe) {
	monetdbe_result* result;
	char* err;
	if ((err = monetdbe_query(mdbe, "SELECT i FROM foo.t", &result, NULL)) != NULL) {
		error(err);
		assert(0);
	}

	monetdbe_column* rcol;
	monetdbe_result_fetch(result, &rcol, 0);

	monetdbe_column_int32_t * col_x = (monetdbe_column_int32_t *) rcol;

	puts("t:");
	for (size_t i = 0 ; i < col_x->count; i++) {
		printf("%d\n", (int32_t) col_x->data[i]);
	}

	if ((err = monetdbe_cleanup_result(mdbe, result)) != NULL) {
		error(err);
		assert(0);
	}
}

monetdbe_database intialize_database() {
	monetdbe_database mdbe;

	if (monetdbe_open(&mdbe, "/home/aris/sources/hammertime/devdb", NULL)) {
		error("Failed to open database")
		assert(0);
	}

	monetdbe_query(mdbe, "CREATE SCHEMA foo", NULL, NULL);
	monetdbe_query(mdbe, "SET SCHEMA foo", NULL, NULL);
	monetdbe_query(mdbe, "CREATE TABLE t (i INTEGER)", NULL, NULL);
	monetdbe_query(mdbe, "INSERT INTO t values (40), (50)", NULL, NULL);

	return mdbe;
}

bool block_creation_second_transaction = true;
bool block_commit_first_transaction = true;
bool block_commit_second_transaction = true;

void* swap_procedure () {
	monetdbe_database mdbe;

	char* err;

	if (monetdbe_open(&mdbe, "/home/aris/sources/hammertime/devdb", NULL)) {
		error(err);
		assert(0);
	}

	if ((err = monetdbe_query(mdbe, "START TRANSACTION", NULL, NULL)) != NULL) {
		error(err);
		assert(0);
	}

	// Now the second transaction can initialize its data structure.
	// This order is to make sure that this first transaction has a lower wstime then the second.
	block_creation_second_transaction = false;

	monetdbe_query(mdbe, "SET SCHEMA foo", NULL, NULL);

	if ((err = monetdbe_query(mdbe, "UPDATE t SET i = 90 - i  WHERE i IN (40, 50)", NULL, NULL)) != NULL) {
		error(err);
		assert(0);
	}

	// Block committing this first transaction until the second transaction has succesfully executed and is ready to be committed.
	// This is to guarantee that the first and second transaction have a common parent.
	while (block_commit_first_transaction) {}

	if ((err = monetdbe_query(mdbe, "COMMIT", NULL, NULL)) != NULL) {
		error(err);
		assert(0);
	}

	// After this first transaction has committed, the second transaction may commit.
	block_commit_second_transaction = false;

	if (monetdbe_close(mdbe)) {
		error("Failed to close database")
		assert(0);
	}

	pthread_exit(NULL);
}

void* update_procedure () {
	monetdbe_database mdbe;

	char* err;

	if (monetdbe_open(&mdbe, "/home/aris/sources/hammertime/devdb", NULL)) {
		error(err);
		assert(0);
	}

	
	while (block_creation_second_transaction) {}

	if ((err = monetdbe_query(mdbe, "START TRANSACTION", NULL, NULL)) != NULL) {
		error(err);
		assert(0);
	}

	monetdbe_query(mdbe, "SET SCHEMA foo", NULL, NULL);

	if ((err = monetdbe_query(mdbe, "TRUNCATE TABLE t", NULL, NULL)) != NULL) {
		error(err);
		assert(0);
	}

	// The first transaction must commit before this second transaction.
	block_commit_first_transaction = false;

	while (block_commit_second_transaction) {}

	if ((err = monetdbe_query(mdbe, "COMMIT", NULL, NULL)) != NULL) {
		error(err);
	}
	else {
		// This commit should have failed because of concurrency conflicts with the first transaction!!!!
		assert(0); 
	}

	if (monetdbe_close(mdbe)) {
		error("Failed to close database")
		assert(0);
	}

	pthread_exit(NULL);
}

int main() {

	monetdbe_database mdbe = intialize_database();

	pthread_t swap_t;
	pthread_t update_t;

	pthread_create(&swap_t, NULL, &swap_procedure, NULL);
	pthread_create(&update_t, NULL, &update_procedure, NULL);

	get_content_of_table(mdbe);

	pthread_join(swap_t, NULL);
	pthread_join(update_t, NULL);

	get_content_of_table(mdbe);

	if (monetdbe_close(mdbe))
		error("Failed to close database")

     return 0;
}
