/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - log_min_messages
 * - backtrace_functions
 * - check_function_bodies
 *--------------------------------------------------------------------
 */

/*--------------------------------------------------------------------
 *
 * guc_tables.c
 *
 * Static tables for the Grand Unified Configuration scheme.
 *
 * Many of these tables are const.  However, ConfigureNamesBool[]
 * and so on are not, because the structs in those arrays are actually
 * the live per-variable state data that guc.c manipulates.  While many of
 * their fields are intended to be constant, some fields change at runtime.
 *
 *
 * Copyright (c) 2000-2024, PostgreSQL Global Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/guc_tables.c
 *
 *--------------------------------------------------------------------
 */
#include "postgres.h"

#include <float.h>
#include <limits.h>
#ifdef HAVE_SYSLOG
#include <syslog.h>
#endif

#include "access/commit_ts.h"
#include "access/gin.h"
#include "access/slru.h"
#include "access/toast_compression.h"
#include "access/twophase.h"
#include "access/xlog_internal.h"
#include "access/xlogprefetcher.h"
#include "access/xlogrecovery.h"
#include "archive/archive_module.h"
#include "catalog/namespace.h"
#include "catalog/storage.h"
#include "commands/async.h"
#include "commands/event_trigger.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/user.h"
#include "commands/vacuum.h"
#include "common/file_utils.h"
#include "common/scram-common.h"
#include "jit/jit.h"
#include "libpq/auth.h"
#include "libpq/libpq.h"
#include "libpq/scram.h"
#include "nodes/queryjumble.h"
#include "optimizer/cost.h"
#include "optimizer/geqo.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "parser/parse_expr.h"
#include "parser/parser.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"
#include "postmaster/startup.h"
#include "postmaster/syslogger.h"
#include "postmaster/walsummarizer.h"
#include "postmaster/walwriter.h"
#include "replication/logicallauncher.h"
#include "replication/slot.h"
#include "replication/slotsync.h"
#include "replication/syncrep.h"
#include "storage/bufmgr.h"
#include "storage/large_object.h"
#include "storage/pg_shmem.h"
#include "storage/predicate.h"
#include "storage/standby.h"
#include "tcop/tcopprot.h"
#include "tsearch/ts_cache.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/float.h"
#include "utils/guc_hooks.h"
#include "utils/guc_tables.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/plancache.h"
#include "utils/ps_status.h"
#include "utils/xml.h"

/* This value is normally passed in from the Makefile */
#ifndef PG_KRB_SRVTAB
#define PG_KRB_SRVTAB ""
#endif

/* XXX these should appear in other modules' header files */
extern bool Log_disconnections;
extern bool Trace_connection_negotiation;
extern int	CommitDelay;
extern int	CommitSiblings;
extern char *default_tablespace;
extern char *temp_tablespaces;
extern bool ignore_checksum_failure;
extern bool ignore_invalid_pages;

#ifdef TRACE_SYNCSCAN
extern bool trace_syncscan;
#endif
#ifdef DEBUG_BOUNDED_SORT
extern bool optimize_bounded_sort;
#endif

/*
 * Options for enum values defined in this module.
 *
 * NOTE! Option values may not contain double quotes!
 */



StaticAssertDecl(lengthof(bytea_output_options) == (BYTEA_OUTPUT_HEX + 2),
				 "array length mismatch");

/*
 * We have different sets for client and server message level options because
 * they sort slightly different (see "log" level), and because "fatal"/"panic"
 * aren't sensible for client_min_messages.
 */








StaticAssertDecl(lengthof(intervalstyle_options) == (INTSTYLE_ISO_8601 + 2),
				 "array length mismatch");



StaticAssertDecl(lengthof(log_error_verbosity_options) == (PGERROR_VERBOSE + 2),
				 "array length mismatch");



StaticAssertDecl(lengthof(log_statement_options) == (LOGSTMT_ALL + 2),
				 "array length mismatch");





StaticAssertDecl(lengthof(session_replication_role_options) == (SESSION_REPLICATION_ROLE_LOCAL + 2),
				 "array length mismatch");

#ifdef HAVE_SYSLOG
#else
#endif



StaticAssertDecl(lengthof(track_function_options) == (TRACK_FUNC_ALL + 2),
				 "array length mismatch");



StaticAssertDecl(lengthof(stats_fetch_consistency) == (PGSTAT_FETCH_CONSISTENCY_SNAPSHOT + 2),
				 "array length mismatch");



StaticAssertDecl(lengthof(xmlbinary_options) == (XMLBINARY_HEX + 2),
				 "array length mismatch");



StaticAssertDecl(lengthof(xmloption_options) == (XMLOPTION_CONTENT + 2),
				 "array length mismatch");

/*
 * Although only "on", "off", and "safe_encoding" are documented, we
 * accept all the likely variants of "on" and "off".
 */


/*
 * Although only "on", "off", and "auto" are documented, we accept
 * all the likely variants of "on" and "off".
 */


/*
 * Although only "on", "off", and "partition" are documented, we
 * accept all the likely variants of "on" and "off".
 */


/*
 * Although only "on", "off", "remote_apply", "remote_write", and "local" are
 * documented, we accept all the likely variants of "on" and "off".
 */


/*
 * Although only "on", "off", "try" are documented, we accept all the likely
 * variants of "on" and "off".
 */
















StaticAssertDecl(lengthof(ssl_protocol_versions_info) == (PG_TLS1_3_VERSION + 2),
				 "array length mismatch");

#ifdef HAVE_SYNCFS
#endif

#ifndef WIN32
#endif
#ifndef EXEC_BACKEND
#endif
#ifdef WIN32
#endif

#ifdef  USE_LZ4
#endif

#ifdef USE_LZ4
#endif
#ifdef USE_ZSTD
#endif

/*
 * Options for enum values stored in other modules
 */
extern const struct config_enum_entry wal_level_options[];
extern const struct config_enum_entry archive_mode_options[];
extern const struct config_enum_entry recovery_target_action_options[];
extern const struct config_enum_entry wal_sync_method_options[];
extern const struct config_enum_entry dynamic_shared_memory_options[];

/*
 * GUC option variables that are exported from this module
 */










	/* this is sort of all three above
											 * together */




__thread bool		check_function_bodies = true;


/*
 * This GUC exists solely for backward compatibility, check its definition for
 * details.
 */




__thread int			log_min_messages = WARNING;









__thread char	   *backtrace_functions;



















/*
 * SSL renegotiation was been removed in PostgreSQL 9.5, but we tolerate it
 * being set to zero (meaning never renegotiate) for backward compatibility.
 * This avoids breaking compatibility with clients that have never supported
 * renegotiation and therefore always try to zero it.
 */


/*
 * This really belongs in pg_shmem.c, but is defined here so that it doesn't
 * need to be duplicated in all the different implementations of pg_shmem.c.
 */




/*
 * These variables are all dummies that don't do anything, except in some
 * cases provide the value for SHOW to display.  The real state is elsewhere
 * and is kept in sync by assign_hooks.
 */










#ifdef HAVE_SYSLOG
#define	DEFAULT_SYSLOG_FACILITY LOG_LOCAL0
#else
#define	DEFAULT_SYSLOG_FACILITY 0
#endif


















#ifdef USE_ASSERT_CHECKING
#define DEFAULT_ASSERT_ENABLED true
#else
#define DEFAULT_ASSERT_ENABLED false
#endif








/* should be static, but commands/variable.c needs to get at this */


/* should be static, but guc.c needs to get at this */



/*
 * Displayable names for context types (enum GucContext)
 *
 * Note: these strings are deliberately not localized.
 */


StaticAssertDecl(lengthof(GucContext_Names) == (PGC_USERSET + 1),
				 "array length mismatch");

/*
 * Displayable names for source types (enum GucSource)
 *
 * Note: these strings are deliberately not localized.
 */


StaticAssertDecl(lengthof(GucSource_Names) == (PGC_S_SESSION + 1),
				 "array length mismatch");

/*
 * Displayable names for the groupings defined in enum config_group
 */


StaticAssertDecl(lengthof(config_group_names) == (DEVELOPER_OPTIONS + 1),
				 "array length mismatch");

/*
 * Displayable names for GUC variable types (enum config_type)
 *
 * Note: these strings are deliberately not localized.
 */


StaticAssertDecl(lengthof(config_type_names) == (PGC_ENUM + 1),
				 "array length mismatch");


/*
 * Contents of GUC tables
 *
 * See src/backend/utils/misc/README for design notes.
 *
 * TO ADD AN OPTION:
 *
 * 1. Declare a global variable of type bool, int, double, or char*
 *	  and make use of it.
 *
 * 2. Decide at what times it's safe to set the option. See guc.h for
 *	  details.
 *
 * 3. Decide on a name, a default value, upper and lower bounds (if
 *	  applicable), etc.
 *
 * 4. Add a record below.
 *
 * 5. Add it to src/backend/utils/misc/postgresql.conf.sample, if
 *	  appropriate.
 *
 * 6. Don't forget to document the option (at least in config.sgml).
 *
 * 7. If it's a new GUC_LIST_QUOTE option, you must add it to
 *	  variable_is_guc_list_quote() in src/bin/pg_dump/dumputils.c.
 */

#ifdef BTREE_BUILD_STATS
#endif
#ifdef LOCK_DEBUG
#endif
#ifdef TRACE_SORT
#endif
#ifdef TRACE_SYNCSCAN
#endif
#ifdef DEBUG_BOUNDED_SORT
#endif
#ifdef WAL_DEBUG
#endif


#ifdef LOCK_DEBUG
#endif
#ifdef DISCARD_CACHES_ENABLED
#if defined(CLOBBER_CACHE_RECURSIVELY)
#else
#endif
#else							/* not DISCARD_CACHES_ENABLED */
#endif							/* not DISCARD_CACHES_ENABLED */





#ifdef USE_SSL
#else
#endif
#ifdef USE_OPENSSL
#else
#endif
#ifdef USE_SSL
#else
#endif



