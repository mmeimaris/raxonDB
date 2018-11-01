# raxonDB
This is the repository for raxonDB, a relational-backed RDF engine based on Extended Characteristic Sets.

In order to load data, you will need to use the class RelationalLoader (for no merging of CSs) or SmartRelationalLoaderArray for merging of CSs.

Postgres needs to be installed and accessible in port 5432 of any machine. Hostname is defined in the parameters.

Example parameter setup for SmartRelationalLoaderArray:

localhost //host for postgres

C:/temp/watdiv.10M.nt //name of RDF file to load (currently only loads nt triples)

testarray //this is the name of the Postgres database

100 //batch size for postgres insert - leave to 100 for best results

postgres //username of postgres user

postgres //password of postgres user

2 //m factor as a multiplier of the mean

To query the data, you will need to use the class RelationalQuerySimple
