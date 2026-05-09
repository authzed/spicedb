/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - GetExtensibleNodeMethods
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * extensible.c
 *	  Support for extensible node types
 *
 * Loadable modules can define what are in effect new types of nodes using
 * the routines in this file.  All such nodes are flagged T_ExtensibleNode,
 * with the extnodename field distinguishing the specific type.  Use
 * RegisterExtensibleNodeMethods to register a new type of extensible node,
 * and GetExtensibleNodeMethods to get information about a previously
 * registered type of extensible node.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/nodes/extensible.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/extensible.h"
#include "utils/hsearch.h"




typedef struct
{
	char		extnodename[EXTNODENAME_MAX_LEN];
	const void *extnodemethods;
} ExtensibleNodeEntry;

/*
 * An internal function to register a new callback structure
 */


/*
 * Register a new type of extensible node.
 */


/*
 * Register a new type of custom scan node
 */


/*
 * An internal routine to get an ExtensibleNodeEntry by the given identifier
 */


/*
 * Get the methods for a given type of extensible node.
 */

const ExtensibleNodeMethods *
GetExtensibleNodeMethods(const char *extnodename, bool missing_ok)
{
	return NULL;
}



/*
 * Get the methods for a given name of CustomScanMethods
 */

