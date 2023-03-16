
# librdf_cassandra

## Overview

This is a plugin for Redland / librdf (see https://librdf.org/), which
supports using a Cassandra store.

## Cassandra

The Apache large-scale distributed, eventually consistent store.

## This plugin

This plugin stores RDF triples in Cassandra by mapping the triples to
rows and storing them in Cassandra.  For each triple, three rows
are stored in Cassandra using a specific encoding which only has
meaning to this plugin.

This is pre-alpha and was used as a demo.  It may not even compile.

## Installation

This is written in C and C++.  C is librdf's native language, and the C
plugin interface wraps a C++ library.

To compile:
```
make
```

To use, the plugin object should be installed in the appropriate
library directory.  This may work for you:
```
make install
```

