/* -*- Mode: c; c-basic-offset: 2 -*-
 *
 * rdf_storage_cassandra.c - RDF Storage using Cassandra implementation
 *
 * Copyright (C) 2004-2010, David Beckett http://www.dajobe.org/
 * Copyright (C) 2004-2005, University of Bristol, UK http://www.bristol.ac.uk/
 * 
 * This package is Free Software and part of Redland http://librdf.org/
 * 
 * It is licensed under the following three licenses as alternatives:
 *   1. GNU Lesser General Public License (LGPL) V2.1 or any newer version
 *   2. GNU General Public License (GPL) V2 or any newer version
 *   3. Apache License, V2.0 or any newer version
 * 
 * You may not use this file except in compliance with at least one of
 * the above three licenses.
 * 
 * See LICENSE.html or LICENSE.txt at the top of this package for the
 * complete terms and further detail along with the license texts for
 * the licenses in COPYING.LIB, COPYING and LICENSE-2.0.txt respectively.
 * 
 * 
 */


#ifdef HAVE_CONFIG_H
#include <rdf_config.h>
#endif

#ifdef WIN32
#include <win32_rdf_config.h>
#endif

#include <stdio.h>
#include <string.h>
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <sys/types.h>

#include <redland.h>
#include <rdf_storage.h>
#include <rdf_heuristics.h>

#include <cassandra.h>

typedef struct
{
    librdf_storage *storage;

    int is_new;
  
    char *name;
    size_t name_len;

    CassSession* session;
    CassCluster* cluster;

} librdf_storage_cassandra_instance;

typedef enum { SPO, POS, OSP } index_type;

/* prototypes for local functions */
static int librdf_storage_cassandra_init(librdf_storage* storage, const char *name, librdf_hash* options);
static int librdf_storage_cassandra_open(librdf_storage* storage, librdf_model* model);
static int librdf_storage_cassandra_close(librdf_storage* storage);
static int librdf_storage_cassandra_size(librdf_storage* storage);
static int librdf_storage_cassandra_add_statement(librdf_storage* storage, librdf_statement* statement);
static int librdf_storage_cassandra_add_statements(librdf_storage* storage, librdf_stream* statement_stream);
static int librdf_storage_cassandra_remove_statement(librdf_storage* storage, librdf_statement* statement);
static int librdf_storage_cassandra_contains_statement(librdf_storage* storage, librdf_statement* statement);
static librdf_stream* librdf_storage_cassandra_serialise(librdf_storage* storage);
static librdf_stream* librdf_storage_cassandra_find_statements(librdf_storage* storage, librdf_statement* statement);

/* serialising implementing functions */
static int cassandra_results_stream_end_of_stream(void* context);
static int cassandra_results_stream_next_statement(void* context);
static void* cassandra_results_stream_get_statement(void* context, int flags);
static void cassandra_results_stream_finished(void* context);

/* context functions */
static int librdf_storage_cassandra_context_add_statement(librdf_storage* storage, librdf_node* context_node, librdf_statement* statement);
static int librdf_storage_cassandra_context_remove_statement(librdf_storage* storage, librdf_node* context_node, librdf_statement* statement);
static int librdf_storage_cassandra_context_contains_statement(librdf_storage* storage, librdf_node* context, librdf_statement* statement);
static librdf_stream* librdf_storage_cassandra_context_serialise(librdf_storage* storage, librdf_node* context_node);

/* helper functions for contexts */

static librdf_iterator* librdf_storage_cassandra_get_contexts(librdf_storage* storage);

/* transactions */
static int librdf_storage_cassandra_transaction_start(librdf_storage *storage);
static int librdf_storage_cassandra_transaction_commit(librdf_storage *storage);
static int librdf_storage_cassandra_transaction_rollback(librdf_storage *storage);

static void librdf_storage_cassandra_register_factory(librdf_storage_factory *factory);
#ifdef MODULAR_LIBRDF
void librdf_storage_module_register_factory(librdf_world *world);
#endif


/* functions implementing storage api */
static int
librdf_storage_cassandra_init(librdf_storage* storage, const char *name,
                           librdf_hash* options)
{

    char *name_copy;
    librdf_storage_cassandra_instance* context;
  
    if(!name) {
	if(options)
	    librdf_free_hash(options);
	return 1;
    }
  
    context = LIBRDF_CALLOC(librdf_storage_cassandra_instance*, 1,
			    sizeof(*context));
    if(!context) {
	if(options)
	    librdf_free_hash(options);
	return 1;
    }

    librdf_storage_set_instance(storage, context);
  
    context->storage = storage;
    context->name_len = strlen(name);
//    context->transaction = 0;

    name_copy = LIBRDF_MALLOC(char*, context->name_len + 1);
    if(!name_copy) {
	if(options)
	    librdf_free_hash(options);
	return 1;
    }

    strcpy(name_copy, name);
    context->name = name_copy;

    // Add options here.

    /* no more options, might as well free them now */
    if(options)
	librdf_free_hash(options);

    // FIXME: Hard-coded;
    context->session = cass_session_new();
    context->cluster = cass_cluster_new();

    cass_cluster_set_contact_points(context->cluster, name);

    CassFuture* future = cass_session_connect(context->session,
					      context->cluster);

    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
	fprintf(stderr, "Cassandra: %s\n", cass_error_desc(rc));
	cass_cluster_free(context->cluster);
	cass_session_free(context->session);
	free(context->name);
	free(context);
	return 1;
    }

    fprintf(stderr, "Connected to Cassandra.\n");

    return 0;

}


static void
librdf_storage_cassandra_terminate(librdf_storage* storage)
{
    librdf_storage_cassandra_instance* context;

    context = (librdf_storage_cassandra_instance*)storage->instance;
  
    if (context == NULL)
	return;

    if(context->name)
	LIBRDF_FREE(char*, context->name);
  
    LIBRDF_FREE(librdf_storage_cassandra_terminate, storage->instance);
}

static
char* node_helper(librdf_storage* storage, librdf_node* node)
{

    librdf_uri* uri;
    librdf_uri* dt_uri;

    const char* integer_type = "http://www.w3.org/2001/XMLSchema#integer";
    const char* float_type = "http://www.w3.org/2001/XMLSchema#float";
    const char* datetime_type = "http://www.w3.org/2001/XMLSchema#dateTime";

    char* name;
    char data_type;

    switch(librdf_node_get_type(node)) {

    case LIBRDF_NODE_TYPE_RESOURCE:
	uri = librdf_node_get_uri(node);
	name = librdf_uri_as_string(uri);
	data_type = 'u';
	break;
	
    case LIBRDF_NODE_TYPE_LITERAL:
	dt_uri = librdf_node_get_literal_value_datatype_uri(node);
	if (dt_uri == 0)
	    data_type = 's';
	else {
	    const char* type_uri = librdf_uri_as_string(dt_uri);
	    if (strcmp(type_uri, integer_type) == 0)
		data_type = 'i';
	    else if (strcmp(type_uri, float_type) == 0)
		data_type = 'f';
	    else if (strcmp(type_uri, datetime_type) == 0)
		data_type = 'd';
	    else
		data_type = 's';
	}
	name = librdf_node_get_literal_value(node);
	break;

    case LIBRDF_NODE_TYPE_BLANK:
	name = librdf_node_get_blank_identifier(node);
	data_type = 'b';
	break;

    case LIBRDF_NODE_TYPE_UNKNOWN:
	break;
	
    }

    char* term = malloc(5 + strlen(name));
    if (term == 0) {
	fprintf(stderr, "malloc failed");
	return 0;
    }
    
    sprintf(term, "%c:%s", data_type, name);

    return term;

}


static int
statement_helper(librdf_storage* storage,
		 librdf_statement* statement,
		 librdf_node* context,
		 char** s, char** p, char** o, char** c)
{

    librdf_node* sn = librdf_statement_get_subject(statement);
    librdf_node* pn = librdf_statement_get_predicate(statement);
    librdf_node* on = librdf_statement_get_object(statement);

    if (sn)
	*s = node_helper(storage, sn);
    else
	*s = 0;
    
    if (pn)
	*p = node_helper(storage, pn);
    else
	*p = 0;

    if (on)
	*o = node_helper(storage, on);
    else
	*o = 0;

    if (context)
	*c = node_helper(storage, context);
    else
	*c = 0;

}

static CassStatement* cassandra_query_(const char* s, const char* p,
					 const char* o)
{

    char* query = "SELECT s, p, o FROM rdf.spo;";
    CassStatement* statement = cass_statement_new(query, 0);
    return statement;

}

static CassStatement* cassandra_query_s(const char* s, const char* p,
					  const char* o)
{

    char* query = "SELECT s, p, o FROM rdf.spo WHERE s = ?;";
    CassStatement* statement = cass_statement_new(query, 1);
    cass_statement_bind_string(statement, 0, s);
    return statement;

}

static CassStatement* cassandra_query_p(const char* s, const char* p,
					  const char* o)
{

    char* query = "SELECT s, p, o FROM rdf.pos WHERE p = ?;";
    CassStatement* statement = cass_statement_new(query, 1);
    cass_statement_bind_string(statement, 0, p);
    return statement;

}

static CassStatement* cassandra_query_o(const char* s, const char* p,
					  const char* o)
{

    char* query = "SELECT s, p, o FROM rdf.osp WHERE o = ?;";
    CassStatement* statement = cass_statement_new(query, 1);
    cass_statement_bind_string(statement, 0, o);
    return statement;

}

static CassStatement* cassandra_query_sp(const char* s, const char* p,
					  const char* o)
{

    char* query = "SELECT s, p, o FROM rdf.spo WHERE s = ? AND p = ?;";
    CassStatement* statement = cass_statement_new(query, 2);
    cass_statement_bind_string(statement, 0, s);
    cass_statement_bind_string(statement, 1, p);
    return statement;

}

static CassStatement* cassandra_query_so(const char* s, const char* p,
					  const char* o)
{

    char* query = "SELECT s, p, o FROM rdf.osp WHERE s = ? AND o = ?;";
    CassStatement* statement = cass_statement_new(query, 2);
    cass_statement_bind_string(statement, 0, s);
    cass_statement_bind_string(statement, 1, o);
    return statement;

}

static CassStatement* cassandra_query_po(const char* s, const char* p,
					  const char* o)
{

    char* query = "SELECT s, p, o FROM rdf.pos WHERE p = ? AND o = ?;";
    CassStatement* statement = cass_statement_new(query, 2);
    cass_statement_bind_string(statement, 0, p);
    cass_statement_bind_string(statement, 1, o);
    return statement;

}

static CassStatement* cassandra_query_spo(const char* s, const char* p,
					  const char* o)
{

    char* query =
	"SELECT s, p, o FROM rdf.pos WHERE s = ? AND p = ? AND o = ?;";
    CassStatement* statement = cass_statement_new(query, 3);
    cass_statement_bind_string(statement, 0, s);
    cass_statement_bind_string(statement, 1, p);
    cass_statement_bind_string(statement, 2, o);
    return statement;

}

static int execute(CassSession* session, char* query)
{

    CassStatement* stmt = cass_statement_new(query, 0);

    CassFuture* future = cass_session_execute(session, stmt);

    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
	fprintf(stderr, "Cassandra error: %s\n", cass_error_desc(rc));
	cass_future_free(future);
	return -1;
    }

    cass_future_free(future);

    return 0;
    
}
  
static int
librdf_storage_cassandra_open(librdf_storage* storage, librdf_model* model)
{

    librdf_storage_cassandra_instance* context;

    context = (librdf_storage_cassandra_instance*)storage->instance;

    char* statement =
	"CREATE KEYSPACE rdf WITH replication = {"
	"  'class': 'SimpleStrategy', 'replication_factor': '1'"
	"};";
    int ret = execute(context->session, statement);
    if (ret < 0) fprintf(stderr, "Error ignored.\n");

    statement =
	"CREATE TABLE rdf.spo ("
	"  s text, p text, o text,"
	"  primary key(s, p, o)"
	");";
    ret = execute(context->session, statement);
    if (ret < 0) fprintf(stderr, "Error ignored.\n");

    statement =
	"CREATE TABLE rdf.pos ("
	"  s text, p text, o text,"
	"  primary key(p, o, s)"
	");";
    ret = execute(context->session, statement);
    if (ret < 0) fprintf(stderr, "Error ignored.\n");

    statement =
	"CREATE TABLE rdf.osp ("
	"  s text, p text, o text,"
	"  primary key(o, s, p)"
	");";
    ret = execute(context->session, statement);
    if (ret < 0) fprintf(stderr, "Error ignored.\n");

    return 0;

}


/**
 * librdf_storage_cassandra_close:
 * @storage: the storage
 *
 * Close the cassandra storage.
 * 
 * Return value: non 0 on failure
 **/
static int
librdf_storage_cassandra_close(librdf_storage* storage)
{
    // FIXME:
#ifdef BROKEN
    librdf_storage_cassandra_instance* context;
    context = (librdf_storage_cassandra_instance*)storage->instance;

    if (context->comms) {
	cassandra_disconnect(context->comms);
	context->comms = 0;
    }

    if (context->transaction)
	cassandra_elements_free(context->transaction);

    return 0;
#endif
}

static int
librdf_storage_cassandra_size(librdf_storage* storage)
{
#ifdef BROKEN

    librdf_storage_cassandra_instance* context =
	(librdf_storage_cassandra_instance*) storage->instance;

    int are_spo;
    int filter;
    char* path;

    cassandra_query* qry = cassandra_query_(0, 0, 0, &are_spo, &filter, &path);

    cassandra_results* res = cassandra_find(context->comms, path, qry);
    if (res == 0) {
	cassandra_query_free(qry);
	fprintf(stderr, "Query execute failed.\n");
	return -1;
    }

    cassandra_results_iterator* iter = cassandra_iterator_create(res);

    cassandra_query_free(qry);

    int count = 0;
    
    while (!cassandra_iterator_done(iter)) {
	const char* a, * b, * c;
	int val;
    
	cassandra_iterator_get(iter, &a, &b, &c, &val);

	if ((val > 0) && (b[0] != '@') && (c[0] != '@'))
	    count++;
	cassandra_iterator_next(iter);
    }

    cassandra_iterator_free(iter);

    cassandra_results_free(res);

    return count;
#endif

}

static int
librdf_storage_cassandra_add_statement(librdf_storage* storage, 
                                    librdf_statement* statement)
{
    return librdf_storage_cassandra_context_add_statement(storage, NULL, statement);
}


static int
librdf_storage_cassandra_add_statements(librdf_storage* storage,
                                     librdf_stream* statement_stream)
{


    librdf_storage_cassandra_instance* context;
    context = (librdf_storage_cassandra_instance*)storage->instance;

    CassBatch* batch = 0;

    const int batch_size = 1000;
    int rows = 0;

    for(; !librdf_stream_end(statement_stream);
	librdf_stream_next(statement_stream)) {

	librdf_statement* statement;
	librdf_node* context_node;
    
	statement = librdf_stream_get_object(statement_stream);
	context_node = librdf_stream_get_context2(statement_stream);

	if(!statement) {
	    break;
	}

	char* s;
	char* p;
	char* o;
	char* c;
	statement_helper(storage, statement, context_node, &s, &p, &o, &c);

	if (batch == 0)
	    batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);

	char* query = "INSERT INTO rdf.spo (s, p, o) VALUES (?, ?, ?);";
	CassStatement* stmt = cass_statement_new(query, 3);
	cass_statement_bind_string(stmt, 0, s);
	cass_statement_bind_string(stmt, 1, p);
	cass_statement_bind_string(stmt, 2, o);
	cass_batch_add_statement(batch, stmt);
	cass_statement_free(stmt);

	query = "INSERT INTO rdf.pos (s, p, o) VALUES (?, ?, ?);";
	stmt = cass_statement_new(query, 3);
	cass_statement_bind_string(stmt, 0, s);
	cass_statement_bind_string(stmt, 1, p);
	cass_statement_bind_string(stmt, 2, o);
	cass_batch_add_statement(batch, stmt);
	cass_statement_free(stmt);

	query = "INSERT INTO rdf.osp (s, p, o) VALUES (?, ?, ?);";
	stmt = cass_statement_new(query, 3);
	cass_statement_bind_string(stmt, 0, s);
	cass_statement_bind_string(stmt, 1, p);
	cass_statement_bind_string(stmt, 2, o);
	cass_batch_add_statement(batch, stmt);
	cass_statement_free(stmt);

	if (++rows > batch_size) {
	    
	    CassFuture* future = cass_session_execute_batch(context->session,
							    batch);
	    cass_batch_free(batch);

	    CassError rc = cass_future_error_code(future);

	    if (rc != CASS_OK) {
		fprintf(stderr, "Cassandra: %s\n", cass_error_desc(rc));
		const char* msg;
		size_t msg_len;
		cass_future_error_message(future, &msg, &msg_len);
		fprintf(stderr, "Cassandra: %*s\n", msg_len, msg);
		cass_future_free(future);
		return -1;
	    }
    
	    cass_future_free(future);

	    batch = 0;

	}

    }
    
    if (batch) {
	    
	CassFuture* future = cass_session_execute_batch(context->session,
							batch);
	cass_batch_free(batch);
	
	CassError rc = cass_future_error_code(future);
	
	if (rc != CASS_OK) {
	    fprintf(stderr, "Cassandra: %s\n", cass_error_desc(rc));
	    const char* msg;
	    size_t msg_len;
	    cass_future_error_message(future, &msg, &msg_len);
	    fprintf(stderr, "Cassandra: %*s\n", msg_len, msg);
	    cass_future_free(future);
	    return -1;
	}
	
	cass_future_free(future);

    }

    return 0;

}


static int
librdf_storage_cassandra_remove_statement(librdf_storage* storage,
                                       librdf_statement* statement)
{
    return librdf_storage_cassandra_context_remove_statement(storage, NULL, 
							  statement);
}

static int
librdf_storage_cassandra_contains_statement(librdf_storage* storage, 
                                         librdf_statement* statement)
{
    return librdf_storage_cassandra_context_contains_statement(storage, NULL,
							    statement);
}


static int
librdf_storage_cassandra_context_contains_statement(librdf_storage* storage,
                                                 librdf_node* context_node,
                                                 librdf_statement* statement)
{

#ifdef BROKEN
    librdf_storage_cassandra_instance* context;
    context = (librdf_storage_cassandra_instance*)storage->instance;

    /* librdf_storage_cassandra_instance* context; */
    cassandra_term terms[4];

    statement_helper(storage, statement, terms, context_node);

    int count = cassandra_count(context->comms, terms[0], terms[1], terms[2]);
    if (count < 0)
	return -1;
    
    return (count > 0);
#endif
}

typedef struct {
    
    librdf_storage *storage;
    librdf_storage_cassandra_instance* cassandra_context;

    librdf_statement *statement;
    librdf_node* context;

    CassStatement* stmt;
    const CassResult* result;
    CassIterator* iter;

    int more_pages;
    int at_end;

} cassandra_results_stream;

static
librdf_node* node_constructor_helper(librdf_world* world, const char* t,
				     size_t len)
{

    librdf_node* o;

    if ((strlen(t) < 2) || (t[1] != ':')) {
	fprintf(stderr, "node_constructor_helper called on invalid term\n");
	return 0;
    }

    if (t[0] == 'u') {
 	o = librdf_new_node_from_counted_uri_string(world,
						    (unsigned char*) t + 2,
						    len - 2);
	return o;
    }

    if (t[0] == 's') {
	o = librdf_new_node_from_typed_counted_literal(world,
						       (unsigned char*) t + 2,
						       len - 2, 0, 0, 0);
	return o;
    }


    if (t[0] == 'i') {
	librdf_uri* dt =
	    librdf_new_uri(world,
			   "http://www.w3.org/2001/XMLSchema#integer");
	if (dt == 0)
	    return 0;

	o = librdf_new_node_from_typed_counted_literal(world, t + 2, len - 2,
						       0, 0, dt);
	librdf_free_uri(dt);
	return o;
    }
    
    if (t[0] == 'f') {
	librdf_uri* dt =
	    librdf_new_uri(world,
			   "http://www.w3.org/2001/XMLSchema#float");
	if (dt == 0)
	    return 0;

	o = librdf_new_node_from_typed_counted_literal(world, t + 2, len - 2,
						       0, 0, dt);
	librdf_free_uri(dt);
	return o;
    }

    if (t[0] == 'd') {
	librdf_uri* dt =
	    librdf_new_uri(world,
			   "http://www.w3.org/2001/XMLSchema#dateTime");
	if (dt == 0)
	    return 0;

	o = librdf_new_node_from_typed_counted_literal(world, t + 2, len - 2,
						       0, 0, dt);
	librdf_free_uri(dt);
	return o;
    }    

    return librdf_new_node_from_typed_counted_literal(world,
						      (unsigned char*) t + 2,
						      len - 2, 0, 0, 0);

}

static int
cassandra_results_stream_end_of_stream(void* context)
{

    cassandra_results_stream* scontext;
    scontext = (cassandra_results_stream*)context;

    if (scontext->at_end) {

	if (scontext->more_pages) {

	    cass_statement_set_paging_state(scontext->stmt,
					    scontext->result);
	    cass_result_free(scontext->result);
	    scontext->result = 0;

	    CassFuture* future = cass_session_execute(scontext->cassandra_context->session,
						      scontext->stmt);

	    CassError rc = cass_future_error_code(future);
	    
	    if (rc != CASS_OK) {
		fprintf(stderr, "Cassandra: %s\n", cass_error_desc(rc));
		const char* msg;
		size_t msg_len;
		cass_future_error_message(future, &msg, &msg_len);
		fprintf(stderr, "Cassandra: %*s\n", msg_len, msg);
		cass_future_free(future);
		return -1;
	    }

	    scontext->result = cass_future_get_result(future);

	    cass_future_free(future);

	    scontext->iter = cass_iterator_from_result(scontext->result);

	    scontext->at_end = !cass_iterator_next(scontext->iter);

	}
	
    }

    return (scontext->at_end);

}


static int
cassandra_results_stream_next_statement(void* context)
{

    cassandra_results_stream* scontext;
    scontext = (cassandra_results_stream*)context;

    CassIterator* iter = scontext->iter;


    if (scontext->at_end) {

	if (scontext->more_pages) {

	    cass_statement_set_paging_state(scontext->stmt,
					    scontext->result);
	    cass_result_free(scontext->result);
	    scontext->result = 0;

	    CassFuture* future = cass_session_execute(scontext->cassandra_context->session,
						      scontext->stmt);

	    CassError rc = cass_future_error_code(future);
	    
	    if (rc != CASS_OK) {
		fprintf(stderr, "Cassandra: %s\n", cass_error_desc(rc));
		const char* msg;
		size_t msg_len;
		cass_future_error_message(future, &msg, &msg_len);
		fprintf(stderr, "Cassandra: %*s\n", msg_len, msg);
		cass_future_free(future);
		return -1;
	    }

	    scontext->result = cass_future_get_result(future);

	    cass_future_free(future);

	    scontext->iter = cass_iterator_from_result(scontext->result);

	    scontext->at_end = !cass_iterator_next(scontext->iter);

	}
	
	if (scontext->at_end)
	    return -1;

	return 0;

    }

    if (scontext->at_end)
	return -1;

    scontext->at_end = !cass_iterator_next(scontext->iter);

    return 0;

}


static void*
cassandra_results_stream_get_statement(void* context, int flags)
{

    cassandra_results_stream* scontext;
    const char* s;
    size_t s_len;
    const char* p;
    size_t p_len;
    const char* o;
    size_t o_len;
    const CassRow* row;
	
    scontext = (cassandra_results_stream*)context;

    switch(flags) {

    case LIBRDF_ITERATOR_GET_METHOD_GET_OBJECT:

	row = cass_iterator_get_row(scontext->iter);

	cass_value_get_string(cass_row_get_column(row, 0), &s, &s_len);
	cass_value_get_string(cass_row_get_column(row, 1), &p, &p_len);
	cass_value_get_string(cass_row_get_column(row, 2), &o, &o_len);

	if (scontext->statement) {
	    librdf_free_statement(scontext->statement);
	    scontext->statement = 0;
	}

	librdf_node* sn, * pn, * on;
	sn = node_constructor_helper(scontext->storage->world, s, s_len);
	pn = node_constructor_helper(scontext->storage->world, p, p_len);
	on = node_constructor_helper(scontext->storage->world, o, o_len);

	if (sn == 0 || pn == 0 || on == 0) {
	    if (sn) librdf_free_node(sn);
	    if (pn) librdf_free_node(pn);
	    if (on) librdf_free_node(on);
	    return 0;
	}

	scontext->statement =
	    librdf_new_statement_from_nodes(scontext->storage->world,
					    sn, pn, on);

	return scontext->statement;

    case LIBRDF_ITERATOR_GET_METHOD_GET_CONTEXT:
	return scontext->context;

    default:
	librdf_log(scontext->storage->world,
		   0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
		   "Unknown iterator method flag %d", flags);
	return NULL;
    }
    
}

static void
cassandra_results_stream_finished(void* context)
{

#ifdef BROKEN
    cassandra_results_stream* scontext;

    scontext  = (cassandra_results_stream*)context;

    if (scontext->iterator) {
	cassandra_iterator_free(scontext->iterator);
	scontext->iterator = 0;
    }

    if (scontext->results) {
	cassandra_results_free(scontext->results);
	scontext->results = 0;
    }
	
    if(scontext->storage)
	librdf_storage_remove_reference(scontext->storage);

    if(scontext->statement)
	librdf_free_statement(scontext->statement);

    if(scontext->context)
	librdf_free_node(scontext->context);

    LIBRDF_FREE(librdf_storage_cassandra_find_statements_stream_context, scontext);

#endif
}

static librdf_stream*
librdf_storage_cassandra_serialise(librdf_storage* storage)
{

    librdf_statement* stmt = 
	    librdf_new_statement_from_nodes(storage->world,
					    0, 0, 0);

    librdf_stream* strm = librdf_storage_cassandra_find_statements(storage,
								   stmt);

    librdf_free_statement(stmt);

    return strm;

#ifdef FIXME
    librdf_storage_cassandra_instance* context =
	(librdf_storage_cassandra_instance*) storage->instance;
    
    context = (librdf_storage_cassandra_instance*)storage->instance;

    cassandra_results_stream* scontext;
    
    scontext =
	LIBRDF_CALLOC(cassandra_results_stream*, 1, sizeof(*scontext));
    if(!scontext)
	return NULL;

    scontext->storage = storage;
    librdf_storage_add_reference(scontext->storage);

    index_type tp;

    cassandra_query* query = cassandra_query_(context->comms, 0, 0, 0, &tp);

    cassandra_iterator* it = cassandra_query_execute(query);

    cassandra_query_free(query);

    if (it == 0) {
	fprintf(stderr, "Failed to execute query.\n");
        return 0;
    }

    scontext->it = it;
    scontext->tp = tp;

    if (cassandra_iterator_has_next(it)) {
	scontext->kv = cassandra_iterator_get_next(it);
	scontext->at_end = 0;
    } else
	scontext->at_end = 1;

    librdf_stream* stream;

    stream =
	librdf_new_stream(storage->world,
			  (void*)scontext,
			  &cassandra_results_stream_end_of_stream,
			  &cassandra_results_stream_next_statement,
			  &cassandra_results_stream_get_statement,
			  &cassandra_results_stream_finished);
    if(!stream) {
	cassandra_results_stream_finished((void*)scontext);
	return NULL;
    }
  
    return stream;
#endif
}


/**
 * librdf_storage_cassandra_find_statements:
 * @storage: the storage
 * @statement: the statement to match
 *
 * .
 * 
 * Return a stream of statements matching the given statement (or
 * all statements if NULL).  Parts (subject, predicate, object) of the
 * statement can be empty in which case any statement part will match that.
 * Uses #librdf_statement_match to do the matching.
 * 
 * Return value: a #librdf_stream or NULL on failure
 **/
static librdf_stream*
librdf_storage_cassandra_find_statements(librdf_storage* storage,
					 librdf_statement* statement)
{
  
    librdf_storage_cassandra_instance* context;
    cassandra_results_stream* scontext;
    librdf_stream* stream;
    char* s;
    char* p;
    char* o;
    char* c;
    
    context = (librdf_storage_cassandra_instance*)storage->instance;

    scontext =
	LIBRDF_CALLOC(cassandra_results_stream*, 1, sizeof(*scontext));
    if(!scontext)
	return NULL;

    scontext->storage = storage;
    librdf_storage_add_reference(scontext->storage);

    scontext->cassandra_context = context;

    statement_helper(storage, statement, 0, &s, &p, &o, &c);
    
    typedef CassStatement* (*query_function)(const char* s, const char* p,
					     const char* o);

    query_function functions[8] = {
	&cassandra_query_,	/* ??? */
	&cassandra_query_s,	/* S?? */
	&cassandra_query_p,	/* ?P? */
	&cassandra_query_sp,	/* SP? */
	&cassandra_query_o,	/* ??O */
	&cassandra_query_so,	/* S?O */
	&cassandra_query_po,	/* ?PO */
	&cassandra_query_spo	/* SPO */
    };

    /* This creates an index into the function table, depending on input
       terms. */
    int num = 0;
    if (o) num += 4;
    if (p) num += 2;
    if (s) num++;

    index_type tp;
    
    query_function fn = functions[num];

    CassStatement* stmt = (*fn)(s, p, o);
    cass_statement_set_paging_size(stmt, 1000);

    CassFuture* future = cass_session_execute(context->session, stmt);

    CassError rc = cass_future_error_code(future);

    if (rc != CASS_OK) {
	fprintf(stderr, "Cassandra: %s\n", cass_error_desc(rc));
	const char* msg;
	size_t msg_len;
	cass_future_error_message(future, &msg, &msg_len);
	fprintf(stderr, "Cassandra: %*s\n", msg_len, msg);
	cass_statement_free(stmt);
	cass_future_free(future);
	return 0;
    }

    const CassResult* result = cass_future_get_result(future);

    CassIterator* iter = cass_iterator_from_result(result);

    /*
    while (cass_iterator_next(iter)) {
	const CassRow* row = cass_iterator_get_row(iter);

	const char *s;
	size_t s_len;

	const char *p;
	size_t p_len;

	const char *o;
	size_t o_len;

	cass_value_get_string(cass_row_get_column(row, 0), &s, &s_len);
	cass_value_get_string(cass_row_get_column(row, 1), &p, &p_len);
	cass_value_get_string(cass_row_get_column(row, 2), &o, &o_len);

	printf("%*s %*s %*s\n", s_len, s, p_len, p, o_len, o);

	printf("ROW\n");
    }

    printf("done\n");
    exit(0);
    */
    
    scontext->stmt = stmt;
    scontext->result = result;
    scontext->iter = iter;

    scontext->more_pages = cass_result_has_more_pages(result);

    scontext->at_end = !cass_iterator_next(scontext->iter);

    stream =
	librdf_new_stream(storage->world,
			  (void*)scontext,
			  &cassandra_results_stream_end_of_stream,
			  &cassandra_results_stream_next_statement,
			  &cassandra_results_stream_get_statement,
			  &cassandra_results_stream_finished);
    if(!stream) {
	cassandra_results_stream_finished((void*)scontext);
	return NULL;
    }
  
    return stream;
    
}

/**
 * librdf_storage_cassandra_context_add_statement:
 * @storage: #librdf_storage object
 * @context_node: #librdf_node object
 * @statement: #librdf_statement statement to add
 *
 * Add a statement to a storage context.
 * 
 * Return value: non 0 on failure
 **/
static int
librdf_storage_cassandra_context_add_statement(librdf_storage* storage,
                                            librdf_node* context_node,
                                            librdf_statement* statement) 
{

    char* s;
    char* p;
    char* o;
    char* c;

    statement_helper(storage, statement, context_node, &s, &p, &o, &c);

    librdf_storage_cassandra_instance* context; 
    context = (librdf_storage_cassandra_instance*)storage->instance;

    char* query =
	"BEGIN BATCH "
	"  INSERT INTO rdf.spo (s, p, o) VALUES (?, ?, ?);" 
	"  INSERT INTO rdf.pos (s, p, o) VALUES (?, ?, ?);"
	"  INSERT INTO rdf.osp (s, p, o) VALUES (?, ?, ?);"
	"APPLY BATCH;";
    CassStatement* stmt = cass_statement_new(query, 9);
    cass_statement_bind_string(stmt, 0, s);
    cass_statement_bind_string(stmt, 1, p);
    cass_statement_bind_string(stmt, 2, o);
    cass_statement_bind_string(stmt, 3, s);
    cass_statement_bind_string(stmt, 4, p);
    cass_statement_bind_string(stmt, 5, o);
    cass_statement_bind_string(stmt, 6, s);
    cass_statement_bind_string(stmt, 7, p);
    cass_statement_bind_string(stmt, 8, o);

    CassFuture* future = cass_session_execute(context->session, stmt);
    cass_statement_free(stmt);

    CassError rc = cass_future_error_code(future);

    if (rc != CASS_OK) {
	fprintf(stderr, "Cassandra: %s\n", cass_error_desc(rc));
	const char* msg;
	size_t msg_len;
	cass_future_error_message(future, &msg, &msg_len);
	fprintf(stderr, "Cassandra: %*s\n", msg_len, msg);
	cass_future_free(future);
	return -1;

    }
    
    cass_future_free(future);

    return 0;

}


/**
 * librdf_storage_cassandra_context_remove_statement:
 * @storage: #librdf_storage object
 * @context_node: #librdf_node object
 * @statement: #librdf_statement statement to remove
 *
 * Remove a statement from a storage context.
 * 
 * Return value: non 0 on failure
 **/
static int
librdf_storage_cassandra_context_remove_statement(librdf_storage* storage, 
                                               librdf_node* context_node,
                                               librdf_statement* statement) 
{

#ifdef BROKEN
    librdf_storage_cassandra_instance* context; 
    context = (librdf_storage_cassandra_instance*)storage->instance;

    char* s;
    char* p;
    char* o;
    char* c;

    statement_helper(storage, statement, context_node, &s, &p, &o, &c);

    int are_spo, filter;
    char* path;
    
    cassandra_query* qry = cassandra_query_spo(s, p, o, &are_spo, &filter, &path);

    cassandra_results* res = cassandra_find(context->comms, path, qry);
    if (res == 0) {
        free(s); free(p); free(o); free(c);
	cassandra_query_free(qry);
	fprintf(stderr, "Query execute failed.\n");
	exit(1);
    }

    cassandra_query_free(qry);

    if (json_object_array_length(res) < 1) {
	free(s); free(p); free(o); free(c);
	cassandra_results_free(res);
	return -1;
    }

    json_object* obj = json_object_array_get_idx(res, 0);
    if (obj == 0) {
	free(s); free(p); free(o); free(c);
	cassandra_results_free(res);
	return -1;
    }

    if (!json_object_object_get_ex(obj, "properties", &obj)) {
	free(s); free(p); free(o); free(c);
	cassandra_results_free(res);
	return -1;
    }

    if (!json_object_object_get_ex(obj, "name", &obj)) {
	free(s); free(p); free(o); free(c);
	cassandra_results_free(res);
	return -1;
    }

    if (!json_object_object_get_ex(obj,
				   "cassandra.function.simple.types.FreqMap",
				   &obj)) {
	free(s); free(p); free(o); free(c);
	cassandra_results_free(res);
	return -1;
    }
      
    if (!json_object_object_get_ex(obj, p, &obj)) {
	free(s); free(p); free(o); free(c);
	cassandra_results_free(res);
	return -1;
    }

    /* This is the value in the freq map. */
    int weight = json_object_get_int(obj);

    cassandra_results_free(res);

    cassandra_elements* elts = cassandra_elements_create();

    /* Create S,O -> P */
    cassandra_add_edge_object(elts, p, s, o, "@r", -weight);

    /* Create S,P -> O */
    cassandra_add_edge_object(elts, o, s, p, "@n", -weight);

    free(s); free(p); free(o); free(c);

    int ret = cassandra_add_elements(context->comms, elts);

    cassandra_elements_free(elts);

    if (ret < 0)
	return -1;

    return 0;

#endif
}


static  int
librdf_storage_cassandra_context_remove_statements(librdf_storage* storage, 
                                                librdf_node* context_node)
{

    //FIXME: Not implemented.

    return -1;

}

/**
 * librdf_storage_cassandra_context_serialise:
 * @storage: #librdf_storage object
 * @context_node: #librdf_node object
 *
 * Cassandra all statements in a storage context.
 * 
 * Return value: #librdf_stream of statements or NULL on failure or context is empty
 **/
static librdf_stream*
librdf_storage_cassandra_context_serialise(librdf_storage* storage,
                                        librdf_node* context_node) 
{

    //FIXME: Not implemented.

    return 0;

}

/**
 * librdf_storage_cassandra_context_get_contexts:
 * @storage: #librdf_storage object
 *
 * Cassandra all context nodes in a storage.
 * 
 * Return value: #librdf_iterator of context_nodes or NULL on failure or no contexts
 **/
static librdf_iterator*
librdf_storage_cassandra_get_contexts(librdf_storage* storage) 
{
    // FIXME: Not implemented.

    return 0;

}

/**
 * librdf_storage_cassandra_get_feature:
 * @storage: #librdf_storage object
 * @feature: #librdf_uri feature property
 *
 * Get the value of a storage feature.
 * 
 * Return value: #librdf_node feature value or NULL if no such feature
 * exists or the value is empty.
 **/
static librdf_node*
librdf_storage_cassandra_get_feature(librdf_storage* storage, librdf_uri* feature)
{
    /* librdf_storage_cassandra_instance* scontext; */
    unsigned char *uri_string;

    /* scontext = (librdf_storage_cassandra_instance*)storage->instance; */

    if(!feature)
	return NULL;

    uri_string = librdf_uri_as_string(feature);
    if(!uri_string)
	return NULL;

    // FIXME: This is a lie.  Contexts not implemented. :-/
    if(!strcmp((const char*)uri_string, LIBRDF_MODEL_FEATURE_CONTEXTS)) {
	return librdf_new_node_from_typed_literal(storage->world,
						  (const unsigned char*)"1",
						  NULL, NULL);
    }

    return NULL;
}


/**
 * librdf_storage_cassandra_transaction_start:
 * @storage: #librdf_storage object
 *
 * Start a new transaction unless one is already active.
 * 
 * Return value: 0 if transaction successfully started, non-0 on error
 * (including a transaction already active)
 **/
static int
librdf_storage_cassandra_transaction_start(librdf_storage *storage)
{
#ifdef BROKEN

    librdf_storage_cassandra_instance* context;

    context = (librdf_storage_cassandra_instance*)storage->instance;

    /* If already have a trasaction, silently do nothing. */
    if (context->transaction)
	return 0;

    context->transaction = cassandra_elements_create();
    if (context->transaction == 0)
	return -1;

    return 0;
#endif

}


/**
 * librdf_storage_cassandra_transaction_commit:
 * @storage: #librdf_storage object
 *
 * Commit an active transaction.
 * 
 * Return value: 0 if transaction successfully committed, non-0 on error
 * (including no transaction active)
 **/
static int
librdf_storage_cassandra_transaction_commit(librdf_storage *storage)
{

#ifdef BROKEN
    librdf_storage_cassandra_instance* context;

    context = (librdf_storage_cassandra_instance*)storage->instance;

    if (context->transaction == 0)
	return -1;

    int ret = cassandra_add_elements(context->comms, context->transaction);

    cassandra_elements_free(context->transaction);

    context->transaction = 0;

    if (ret < 0) return -1;

    return 0;

#endif
}


/**
 * librdf_storage_cassandra_transaction_rollback:
 * @storage: #librdf_storage object
 *
 * Roll back an active transaction.
 * 
 * Return value: 0 if transaction successfully committed, non-0 on error
 * (including no transaction active)
 **/
static int
librdf_storage_cassandra_transaction_rollback(librdf_storage *storage)
{
#ifdef BROKEN

    librdf_storage_cassandra_instance* context;

    context = (librdf_storage_cassandra_instance*)storage->instance;

    if (context->transaction)
	return -1;

    cassandra_elements_free(context->transaction);

    context->transaction = 0;

    return 0;
#endif

}

/** Local entry point for dynamically loaded storage module */
static void
librdf_storage_cassandra_register_factory(librdf_storage_factory *factory) 
{
    LIBRDF_ASSERT_CONDITION(!strcmp(factory->name, "cassandra"));

    factory->version            = LIBRDF_STORAGE_INTERFACE_VERSION;
    factory->init               = librdf_storage_cassandra_init;
    factory->terminate          = librdf_storage_cassandra_terminate;
    factory->open               = librdf_storage_cassandra_open;
    factory->close              = librdf_storage_cassandra_close;
    factory->size               = librdf_storage_cassandra_size;
    factory->add_statement      = librdf_storage_cassandra_add_statement;
    factory->add_statements     = librdf_storage_cassandra_add_statements;
    factory->remove_statement   = librdf_storage_cassandra_remove_statement;
    factory->contains_statement = librdf_storage_cassandra_contains_statement;
    factory->serialise          = librdf_storage_cassandra_serialise;
    factory->find_statements    = librdf_storage_cassandra_find_statements;
    factory->context_add_statement    = librdf_storage_cassandra_context_add_statement;
    factory->context_remove_statement = librdf_storage_cassandra_context_remove_statement;
    factory->context_remove_statements = librdf_storage_cassandra_context_remove_statements;
    factory->context_serialise        = librdf_storage_cassandra_context_serialise;
    factory->get_contexts             = librdf_storage_cassandra_get_contexts;
    factory->get_feature              = librdf_storage_cassandra_get_feature;
    factory->transaction_start        = librdf_storage_cassandra_transaction_start;
    factory->transaction_commit       = librdf_storage_cassandra_transaction_commit;
    factory->transaction_rollback     = librdf_storage_cassandra_transaction_rollback;
}

#ifdef MODULAR_LIBRDF

/** Entry point for dynamically loaded storage module */
void
librdf_storage_module_register_factory(librdf_world *world)
{
    librdf_storage_register_factory(world, "cassandra", "Cassandra",
				    &librdf_storage_cassandra_register_factory);
}

#else

/*
 * librdf_init_storage_cassandra:
 * @world: world object
 *
 * INTERNAL - Initialise the built-in storage_cassandra module.
 */
void
librdf_init_storage_cassandra(librdf_world *world)
{
    librdf_storage_register_factory(world, "cassandra", "Cassandra",
				    &librdf_storage_cassandra_register_factory);
}

#endif

