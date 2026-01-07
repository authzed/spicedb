CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER spicedb_fdw FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'hostofproxy', dbname 'default', port '5432', use_remote_estimate 'true');

CREATE USER MAPPING FOR postgres SERVER spicedb_fdw OPTIONS (user 'proxyusername', password 'proxypassword');

CREATE SCHEMA spicedb;

-- IMPORT FOREIGN SCHEMA spicedb FROM SERVER spicedb_fdw INTO spicedb;

CREATE FOREIGN TABLE relationships ( 
    resource_type text NOT NULL,
    resource_id text NOT NULL,
    relation text NOT NULL,
    subject_type text NOT NULL,
    subject_id text NOT NULL,
    optional_subject_relation text default '',
    caveat_name text default '',
    caveat_context text default '',
    consistency text default ''
) SERVER spicedb_fdw;

CREATE FOREIGN TABLE permissions (
    resource_type text NOT NULL,
    resource_id text NOT NULL,
    permission text NOT NULL,
    subject_type text NOT NULL,
    subject_id text NOT NULL,
    optional_subject_relation text NOT NULL,
    has_permission boolean, -- NULL when maybe
    consistency text default ''
) SERVER spicedb_fdw;

CREATE FOREIGN TABLE schema ( schema_text text ) SERVER spicedb_fdw;