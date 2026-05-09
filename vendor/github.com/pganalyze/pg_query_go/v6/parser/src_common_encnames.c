/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - pg_enc2name_tbl
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * encnames.c
 *	  Encoding names and routines for working with them.
 *
 * Portions Copyright (c) 2001-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/encnames.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"

#include <ctype.h>
#include <unistd.h>

#include "mb/pg_wchar.h"


/* ----------
 * All encoding names, sorted:		 *** A L P H A B E T I C ***
 *
 * All names must be without irrelevant chars, search routines use
 * isalnum() chars only. It means ISO-8859-1, iso_8859-1 and Iso8859_1
 * are always converted to 'iso88591'. All must be lower case.
 *
 * The table doesn't contain 'cs' aliases (like csISOLatin1). It's needed?
 *
 * Karel Zak, Aug 2001
 * ----------
 */
typedef struct pg_encname
{
	const char *name;
	pg_enc		encoding;
} pg_encname;



/* ----------
 * These are "official" encoding names.
 * ----------
 */
#ifndef WIN32
#define DEF_ENC2NAME(name, codepage) { #name, PG_##name }
#else
#define DEF_ENC2NAME(name, codepage) { #name, PG_##name, codepage }
#endif

const pg_enc2name pg_enc2name_tbl[] =
{
	[PG_SQL_ASCII] = DEF_ENC2NAME(SQL_ASCII, 0),
	[PG_EUC_JP] = DEF_ENC2NAME(EUC_JP, 20932),
	[PG_EUC_CN] = DEF_ENC2NAME(EUC_CN, 20936),
	[PG_EUC_KR] = DEF_ENC2NAME(EUC_KR, 51949),
	[PG_EUC_TW] = DEF_ENC2NAME(EUC_TW, 0),
	[PG_EUC_JIS_2004] = DEF_ENC2NAME(EUC_JIS_2004, 20932),
	[PG_UTF8] = DEF_ENC2NAME(UTF8, 65001),
	[PG_MULE_INTERNAL] = DEF_ENC2NAME(MULE_INTERNAL, 0),
	[PG_LATIN1] = DEF_ENC2NAME(LATIN1, 28591),
	[PG_LATIN2] = DEF_ENC2NAME(LATIN2, 28592),
	[PG_LATIN3] = DEF_ENC2NAME(LATIN3, 28593),
	[PG_LATIN4] = DEF_ENC2NAME(LATIN4, 28594),
	[PG_LATIN5] = DEF_ENC2NAME(LATIN5, 28599),
	[PG_LATIN6] = DEF_ENC2NAME(LATIN6, 0),
	[PG_LATIN7] = DEF_ENC2NAME(LATIN7, 0),
	[PG_LATIN8] = DEF_ENC2NAME(LATIN8, 0),
	[PG_LATIN9] = DEF_ENC2NAME(LATIN9, 28605),
	[PG_LATIN10] = DEF_ENC2NAME(LATIN10, 0),
	[PG_WIN1256] = DEF_ENC2NAME(WIN1256, 1256),
	[PG_WIN1258] = DEF_ENC2NAME(WIN1258, 1258),
	[PG_WIN866] = DEF_ENC2NAME(WIN866, 866),
	[PG_WIN874] = DEF_ENC2NAME(WIN874, 874),
	[PG_KOI8R] = DEF_ENC2NAME(KOI8R, 20866),
	[PG_WIN1251] = DEF_ENC2NAME(WIN1251, 1251),
	[PG_WIN1252] = DEF_ENC2NAME(WIN1252, 1252),
	[PG_ISO_8859_5] = DEF_ENC2NAME(ISO_8859_5, 28595),
	[PG_ISO_8859_6] = DEF_ENC2NAME(ISO_8859_6, 28596),
	[PG_ISO_8859_7] = DEF_ENC2NAME(ISO_8859_7, 28597),
	[PG_ISO_8859_8] = DEF_ENC2NAME(ISO_8859_8, 28598),
	[PG_WIN1250] = DEF_ENC2NAME(WIN1250, 1250),
	[PG_WIN1253] = DEF_ENC2NAME(WIN1253, 1253),
	[PG_WIN1254] = DEF_ENC2NAME(WIN1254, 1254),
	[PG_WIN1255] = DEF_ENC2NAME(WIN1255, 1255),
	[PG_WIN1257] = DEF_ENC2NAME(WIN1257, 1257),
	[PG_KOI8U] = DEF_ENC2NAME(KOI8U, 21866),
	[PG_SJIS] = DEF_ENC2NAME(SJIS, 932),
	[PG_BIG5] = DEF_ENC2NAME(BIG5, 950),
	[PG_GBK] = DEF_ENC2NAME(GBK, 936),
	[PG_UHC] = DEF_ENC2NAME(UHC, 949),
	[PG_GB18030] = DEF_ENC2NAME(GB18030, 54936),
	[PG_JOHAB] = DEF_ENC2NAME(JOHAB, 0),
	[PG_SHIFT_JIS_2004] = DEF_ENC2NAME(SHIFT_JIS_2004, 932),
};

/* ----------
 * These are encoding names for gettext.
 *
 * This covers all encodings except MULE_INTERNAL, which is alien to gettext.
 * ----------
 */



/*
 * Table of encoding names for ICU (currently covers backend encodings only)
 *
 * Reference: <https://ssl.icu-project.org/icu-bin/convexp>
 *
 * NULL entries are not supported by ICU, or their mapping is unclear.
 */


StaticAssertDecl(lengthof(pg_enc2icu_tbl) == PG_ENCODING_BE_LAST + 1,
				 "pg_enc2icu_tbl incomplete");


/*
 * Is this encoding supported by ICU?
 */


/*
 * Returns ICU's name for encoding, or NULL if not supported
 */



/* ----------
 * Encoding checks, for error returns -1 else encoding id
 * ----------
 */






/*
 * Remove irrelevant chars from encoding name, store at *newkey
 *
 * (Caller's responsibility to provide a large enough buffer)
 */


/*
 * Search encoding by encoding name
 *
 * Returns encoding ID, or -1 if not recognized
 */



