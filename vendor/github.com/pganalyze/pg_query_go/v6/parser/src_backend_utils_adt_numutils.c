/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - pg_strtoint32_safe
 * - hexlookup
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * numutils.c
 *	  utility functions for I/O of built-in numeric types.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/numutils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include <limits.h>
#include <ctype.h>

#include "port/pg_bitutils.h"
#include "utils/builtins.h"

/*
 * A table of all two-digit numbers. This is used to speed up decimal digit
 * generation by copying pairs of digits into the final output.
 */


/*
 * Adapted from http://graphics.stanford.edu/~seander/bithacks.html#IntegerLog10
 */




static const int8 hexlookup[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

/*
 * Convert input string to a signed 16 bit integer.  Input strings may be
 * expressed in base-10, hexadecimal, octal, or binary format, all of which
 * can be prefixed by an optional sign character, either '+' (the default) or
 * '-' for negative numbers.  Hex strings are recognized by the digits being
 * prefixed by 0x or 0X while octal strings are recognized by the 0o or 0O
 * prefix.  The binary representation is recognized by the 0b or 0B prefix.
 *
 * Allows any number of leading or trailing whitespace characters.  Digits may
 * optionally be separated by a single underscore character.  These can only
 * come between digits and not before or after the digits.  Underscores have
 * no effect on the return value and are supported only to assist in improving
 * the human readability of the input strings.
 *
 * pg_strtoint16() will throw ereport() upon bad input format or overflow;
 * while pg_strtoint16_safe() instead returns such complaints in *escontext,
 * if it's an ErrorSaveContext.
*
 * NB: Accumulate input as an unsigned number, to deal with two's complement
 * representation of the most negative number, which can't be represented as a
 * signed positive number.
 */




/*
 * Convert input string to a signed 32 bit integer.  Input strings may be
 * expressed in base-10, hexadecimal, octal, or binary format, all of which
 * can be prefixed by an optional sign character, either '+' (the default) or
 * '-' for negative numbers.  Hex strings are recognized by the digits being
 * prefixed by 0x or 0X while octal strings are recognized by the 0o or 0O
 * prefix.  The binary representation is recognized by the 0b or 0B prefix.
 *
 * Allows any number of leading or trailing whitespace characters.  Digits may
 * optionally be separated by a single underscore character.  These can only
 * come between digits and not before or after the digits.  Underscores have
 * no effect on the return value and are supported only to assist in improving
 * the human readability of the input strings.
 *
 * pg_strtoint32() will throw ereport() upon bad input format or overflow;
 * while pg_strtoint32_safe() instead returns such complaints in *escontext,
 * if it's an ErrorSaveContext.
 *
 * NB: Accumulate input as an unsigned number, to deal with two's complement
 * representation of the most negative number, which can't be represented as a
 * signed positive number.
 */


int32
pg_strtoint32_safe(const char *s, Node *escontext)
{
	const char *ptr = s;
	const char *firstdigit;
	uint32		tmp = 0;
	bool		neg = false;
	unsigned char digit;

	/*
	 * The majority of cases are likely to be base-10 digits without any
	 * underscore separator characters.  We'll first try to parse the string
	 * with the assumption that's the case and only fallback on a slower
	 * implementation which handles hex, octal and binary strings and
	 * underscores if the fastpath version cannot parse the string.
	 */

	/* leave it up to the slow path to look for leading spaces */

	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}

	/* a leading '+' is uncommon so leave that for the slow path */

	/* process the first digit */
	digit = (*ptr - '0');

	/*
	 * Exploit unsigned arithmetic to save having to check both the upper and
	 * lower bounds of the digit.
	 */
	if (likely(digit < 10))
	{
		ptr++;
		tmp = digit;
	}
	else
	{
		/* we need at least one digit */
		goto slow;
	}

	/* process remaining digits */
	for (;;)
	{
		digit = (*ptr - '0');

		if (digit >= 10)
			break;

		ptr++;

		if (unlikely(tmp > -(PG_INT32_MIN / 10)))
			goto out_of_range;

		tmp = tmp * 10 + digit;
	}

	/* when the string does not end in a digit, let the slow path handle it */
	if (unlikely(*ptr != '\0'))
		goto slow;

	if (neg)
	{
		/* check the negative equivalent will fit without overflowing */
		if (unlikely(tmp > (uint32) (-(PG_INT32_MIN + 1)) + 1))
			goto out_of_range;
		return -((int32) tmp);
	}

	if (unlikely(tmp > PG_INT32_MAX))
		goto out_of_range;

	return (int32) tmp;

slow:
	tmp = 0;
	ptr = s;
	/* no need to reset neg */

	/* skip leading spaces */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}
	else if (*ptr == '+')
		ptr++;

	/* process digits */
	if (ptr[0] == '0' && (ptr[1] == 'x' || ptr[1] == 'X'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (isxdigit((unsigned char) *ptr))
			{
				if (unlikely(tmp > -(PG_INT32_MIN / 16)))
					goto out_of_range;

				tmp = tmp * 16 + hexlookup[(unsigned char) *ptr++];
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || !isxdigit((unsigned char) *ptr))
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (ptr[0] == '0' && (ptr[1] == 'o' || ptr[1] == 'O'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '7')
			{
				if (unlikely(tmp > -(PG_INT32_MIN / 8)))
					goto out_of_range;

				tmp = tmp * 8 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || *ptr < '0' || *ptr > '7')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (ptr[0] == '0' && (ptr[1] == 'b' || ptr[1] == 'B'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '1')
			{
				if (unlikely(tmp > -(PG_INT32_MIN / 2)))
					goto out_of_range;

				tmp = tmp * 2 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || *ptr < '0' || *ptr > '1')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else
	{
		firstdigit = ptr;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '9')
			{
				if (unlikely(tmp > -(PG_INT32_MIN / 10)))
					goto out_of_range;

				tmp = tmp * 10 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore may not be first */
				if (unlikely(ptr == firstdigit))
					goto invalid_syntax;
				/* and it must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || !isdigit((unsigned char) *ptr))
					goto invalid_syntax;
			}
			else
				break;
		}
	}

	/* require at least one digit */
	if (unlikely(ptr == firstdigit))
		goto invalid_syntax;

	/* allow trailing whitespace, but not other trailing chars */
	while (isspace((unsigned char) *ptr))
		ptr++;

	if (unlikely(*ptr != '\0'))
		goto invalid_syntax;

	if (neg)
	{
		/* check the negative equivalent will fit without overflowing */
		if (tmp > (uint32) (-(PG_INT32_MIN + 1)) + 1)
			goto out_of_range;
		return -((int32) tmp);
	}

	if (tmp > PG_INT32_MAX)
		goto out_of_range;

	return (int32) tmp;

out_of_range:
	ereturn(escontext, 0,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value \"%s\" is out of range for type %s",
					s, "integer")));

invalid_syntax:
	ereturn(escontext, 0,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"integer", s)));
}

/*
 * Convert input string to a signed 64 bit integer.  Input strings may be
 * expressed in base-10, hexadecimal, octal, or binary format, all of which
 * can be prefixed by an optional sign character, either '+' (the default) or
 * '-' for negative numbers.  Hex strings are recognized by the digits being
 * prefixed by 0x or 0X while octal strings are recognized by the 0o or 0O
 * prefix.  The binary representation is recognized by the 0b or 0B prefix.
 *
 * Allows any number of leading or trailing whitespace characters.  Digits may
 * optionally be separated by a single underscore character.  These can only
 * come between digits and not before or after the digits.  Underscores have
 * no effect on the return value and are supported only to assist in improving
 * the human readability of the input strings.
 *
 * pg_strtoint64() will throw ereport() upon bad input format or overflow;
 * while pg_strtoint64_safe() instead returns such complaints in *escontext,
 * if it's an ErrorSaveContext.
 *
 * NB: Accumulate input as an unsigned number, to deal with two's complement
 * representation of the most negative number, which can't be represented as a
 * signed positive number.
 */




/*
 * Convert input string to an unsigned 32 bit integer.
 *
 * Allows any number of leading or trailing whitespace characters.
 *
 * If endloc isn't NULL, store a pointer to the rest of the string there,
 * so that caller can parse the rest.  Otherwise, it's an error if anything
 * but whitespace follows.
 *
 * typname is what is reported in error messages.
 *
 * If escontext points to an ErrorSaveContext node, that is filled instead
 * of throwing an error; the caller must check SOFT_ERROR_OCCURRED()
 * to detect errors.
 */
#if PG_UINT32_MAX != ULONG_MAX
#endif

/*
 * Convert input string to an unsigned 64 bit integer.
 *
 * Allows any number of leading or trailing whitespace characters.
 *
 * If endloc isn't NULL, store a pointer to the rest of the string there,
 * so that caller can parse the rest.  Otherwise, it's an error if anything
 * but whitespace follows.
 *
 * typname is what is reported in error messages.
 *
 * If escontext points to an ErrorSaveContext node, that is filled instead
 * of throwing an error; the caller must check SOFT_ERROR_OCCURRED()
 * to detect errors.
 */


/*
 * pg_itoa: converts a signed 16-bit integer to its string representation
 * and returns strlen(a).
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least 7 bytes, counting a leading sign and trailing NUL).
 *
 * It doesn't seem worth implementing this separately.
 */


/*
 * pg_ultoa_n: converts an unsigned 32-bit integer to its string representation,
 * not NUL-terminated, and returns the length of that string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result (at
 * least 10 bytes)
 */


/*
 * pg_ltoa: converts a signed 32-bit integer to its string representation and
 * returns strlen(a).
 *
 * It is the caller's responsibility to ensure that a is at least 12 bytes long,
 * which is enough room to hold a minus sign, a maximally long int32, and the
 * above terminating NUL.
 */


/*
 * Get the decimal representation, not NUL-terminated, and return the length of
 * same.  Caller must ensure that a points to at least MAXINT8LEN bytes.
 */


/*
 * pg_lltoa: converts a signed 64-bit integer to its string representation and
 * returns strlen(a).
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least MAXINT8LEN + 1 bytes, counting a leading sign and trailing NUL).
 */



/*
 * pg_ultostr_zeropad
 *		Converts 'value' into a decimal string representation stored at 'str'.
 *		'minwidth' specifies the minimum width of the result; any extra space
 *		is filled up by prefixing the number with zeros.
 *
 * Returns the ending address of the string result (the last character written
 * plus 1).  Note that no NUL terminator is written.
 *
 * The intended use-case for this function is to build strings that contain
 * multiple individual numbers, for example:
 *
 *	str = pg_ultostr_zeropad(str, hours, 2);
 *	*str++ = ':';
 *	str = pg_ultostr_zeropad(str, mins, 2);
 *	*str++ = ':';
 *	str = pg_ultostr_zeropad(str, secs, 2);
 *	*str = '\0';
 *
 * Note: Caller must ensure that 'str' points to enough memory to hold the
 * result.
 */


/*
 * pg_ultostr
 *		Converts 'value' into a decimal string representation stored at 'str'.
 *
 * Returns the ending address of the string result (the last character written
 * plus 1).  Note that no NUL terminator is written.
 *
 * The intended use-case for this function is to build strings that contain
 * multiple individual numbers, for example:
 *
 *	str = pg_ultostr(str, a);
 *	*str++ = ' ';
 *	str = pg_ultostr(str, b);
 *	*str = '\0';
 *
 * Note: Caller must ensure that 'str' points to enough memory to hold the
 * result.
 */

