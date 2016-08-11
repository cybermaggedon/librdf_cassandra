
CFLAGS=-I/usr/include/raptor2 -I/usr/include/rasqal -fPIC -I. -Iredland -g
CXXFLAGS=-I/usr/include/raptor2 -I/usr/include/rasqal -fPIC -I. -Iredland -g
LIBS=-lrasqal -lrdf

SQLITE_FLAGS=-DSTORE=\"sqlite\" -DSTORE_NAME=\"STORE.db\"

CASSANDRA_FLAGS=-DSTORE=\"cassandra\" -DSTORE_NAME=\"gaffer:42424\"

#LIB_OBJS= 
#cassandra.o \
#	cassandra_comms.o

all: test-sqlite test-cassandra librdf_storage_cassandra.so

#libcassandra.a: ${LIB_OBJS}
#	${AR} cr $@ ${LIB_OBJS}
#	ranlib $@

test-sqlite: test-sqlite.o
	${CXX} ${CXXFLAGS} test-sqlite.o -o $@ ${LIBS}

test-cassandra: test-cassandra.o
	${CXX} ${CXXFLAGS} test-cassandra.o -o $@ ${LIBS}

bulk_load: bulk_load.o
	${CXX} ${CXXFLAGS} bulk_load.o -o $@ ${LIBS}

test-sqlite.o: test.C
	${CXX} ${CXXFLAGS} -c $< -o $@  ${SQLITE_FLAGS}

test-cassandra.o: test.C
	${CXX} ${CXXFLAGS} -c $< -o $@ ${CASSANDRA_FLAGS}

CASSANDRA_OBJECTS=cassandra.o libcassandra.a

librdf_storage_cassandra.so: ${CASSANDRA_OBJECTS}
	${CXX} ${CXXFLAGS} -shared -o $@ ${CASSANDRA_OBJECTS} -lthrift

cassandra.o: CFLAGS += -DHAVE_CONFIG_H -DLIBRDF_INTERNAL=1 

install: all
	sudo cp librdf_storage_cassandra.so /usr/lib64/redland

depend:
	makedepend -Y -I. *.c *.C

# DO NOT DELETE

gaffer.o: ./gaffer_comms.h ./gaffer_query.h
gaffer_comms.o: ./gaffer_comms.h ./gaffer_query.h
gaffer_query.o: ./gaffer_query.h
