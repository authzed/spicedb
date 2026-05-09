/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - bms_copy
 * - bms_is_valid_set
 * - bms_equal
 * - bms_free
 * - bms_next_member
 * - bms_num_members
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * bitmapset.c
 *	  PostgreSQL generic bitmap set package
 *
 * A bitmap set can represent any set of nonnegative integers, although
 * it is mainly intended for sets where the maximum value is not large,
 * say at most a few hundred.  By convention, we always represent a set with
 * the minimum possible number of words, i.e, there are never any trailing
 * zero words.  Enforcing this requires that an empty set is represented as
 * NULL.  Because an empty Bitmapset is represented as NULL, a non-NULL
 * Bitmapset always has at least 1 Bitmapword.  We can exploit this fact to
 * speed up various loops over the Bitmapset's words array by using "do while"
 * loops instead of "for" loops.  This means the code does not waste time
 * checking the loop condition before the first iteration.  For Bitmapsets
 * containing only a single word (likely the majority of them) this halves the
 * number of loop condition checks.
 *
 * Callers must ensure that the set returned by functions in this file which
 * adjust the members of an existing set is assigned to all pointers pointing
 * to that existing set.  No guarantees are made that we'll ever modify the
 * existing set in-place and return it.
 *
 * To help find bugs caused by callers failing to record the return value of
 * the function which manipulates an existing set, we support building with
 * REALLOCATE_BITMAPSETS.  This results in the set being reallocated each time
 * the set is altered and the existing being pfreed.  This is useful as if any
 * references still exist to the old set, we're more likely to notice as
 * any users of the old set will be accessing pfree'd memory.  This option is
 * only intended to be used for debugging.
 *
 * Copyright (c) 2003-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/nodes/bitmapset.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/hashfn.h"
#include "nodes/bitmapset.h"
#include "nodes/pg_list.h"
#include "port/pg_bitutils.h"


#define WORDNUM(x)	((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)	((x) % BITS_PER_BITMAPWORD)

#define BITMAPSET_SIZE(nwords)	\
	(offsetof(Bitmapset, words) + (nwords) * sizeof(bitmapword))

/*----------
 * This is a well-known cute trick for isolating the rightmost one-bit
 * in a word.  It assumes two's complement arithmetic.  Consider any
 * nonzero value, and focus attention on the rightmost one.  The value is
 * then something like
 *				xxxxxx10000
 * where x's are unspecified bits.  The two's complement negative is formed
 * by inverting all the bits and adding one.  Inversion gives
 *				yyyyyy01111
 * where each y is the inverse of the corresponding x.  Incrementing gives
 *				yyyyyy10000
 * and then ANDing with the original value gives
 *				00000010000
 * This works for all cases except original value = zero, where of course
 * we get zero.
 *----------
 */
#define RIGHTMOST_ONE(x) ((signedbitmapword) (x) & -((signedbitmapword) (x)))

#define HAS_MULTIPLE_ONES(x)	((bitmapword) RIGHTMOST_ONE(x) != (x))

#ifdef USE_ASSERT_CHECKING
/*
 * bms_is_valid_set - for cassert builds to check for valid sets
 */
static bool
bms_is_valid_set(const Bitmapset *a)
{
	/* NULL is the correct representation of an empty set */
	if (a == NULL)
		return true;

	/* check the node tag is set correctly.  pfree'd pointer, maybe? */
	if (!IsA(a, Bitmapset))
		return false;

	/* trailing zero words are not allowed */
	if (a->words[a->nwords - 1] == 0)
		return false;

	return true;
}
#endif

#ifdef REALLOCATE_BITMAPSETS
/*
 * bms_copy_and_free
 *		Only required in REALLOCATE_BITMAPSETS builds.  Provide a simple way
 *		to return a freshly allocated set and pfree the original.
 *
 * Note: callers which accept multiple sets must be careful when calling this
 * function to clone one parameter as other parameters may point to the same
 * set.  A good option is to call this just before returning the resulting
 * set.
 */
static Bitmapset *
bms_copy_and_free(Bitmapset *a)
{
	Bitmapset  *c = bms_copy(a);

	bms_free(a);
	return c;
}
#endif

/*
 * bms_copy - make a palloc'd copy of a bitmapset
 */
Bitmapset *
bms_copy(const Bitmapset *a)
{
	Bitmapset  *result;
	size_t		size;

	Assert(bms_is_valid_set(a));

	if (a == NULL)
		return NULL;

	size = BITMAPSET_SIZE(a->nwords);
	result = (Bitmapset *) palloc(size);
	memcpy(result, a, size);
	return result;
}

/*
 * bms_equal - are two bitmapsets equal? or both NULL?
 */
bool
bms_equal(const Bitmapset *a, const Bitmapset *b)
{
	int			i;

	Assert(bms_is_valid_set(a));
	Assert(bms_is_valid_set(b));

	/* Handle cases where either input is NULL */
	if (a == NULL)
	{
		if (b == NULL)
			return true;
		return false;
	}
	else if (b == NULL)
		return false;

	/* can't be equal if the word counts don't match */
	if (a->nwords != b->nwords)
		return false;

	/* check each word matches */
	i = 0;
	do
	{
		if (a->words[i] != b->words[i])
			return false;
	} while (++i < a->nwords);

	return true;
}

/*
 * bms_compare - qsort-style comparator for bitmapsets
 *
 * This guarantees to report values as equal iff bms_equal would say they are
 * equal.  Otherwise, the highest-numbered bit that is set in one value but
 * not the other determines the result.  (This rule means that, for example,
 * {6} is greater than {5}, which seems plausible.)
 */


/*
 * bms_make_singleton - build a bitmapset containing a single member
 */


/*
 * bms_free - free a bitmapset
 *
 * Same as pfree except for allowing NULL input
 */
void
bms_free(Bitmapset *a)
{
	if (a)
		pfree(a);
}


/*
 * bms_union - create and return a new set containing all members from both
 * input sets.  Both inputs are left unmodified.
 */


/*
 * bms_intersect - create and return a new set containing members which both
 * input sets have in common.  Both inputs are left unmodified.
 */


/*
 * bms_difference - create and return a new set containing all the members of
 * 'a' without the members of 'b'.
 */


/*
 * bms_is_subset - is A a subset of B?
 */


/*
 * bms_subset_compare - compare A and B for equality/subset relationships
 *
 * This is more efficient than testing bms_is_subset in both directions.
 */


/*
 * bms_is_member - is X a member of A?
 */


/*
 * bms_member_index
 *		determine 0-based index of member x in the bitmap
 *
 * Returns (-1) when x is not a member.
 */


/*
 * bms_overlap - do sets overlap (ie, have a nonempty intersection)?
 */


/*
 * bms_overlap_list - does a set overlap an integer list?
 */


/*
 * bms_nonempty_difference - do sets have a nonempty difference?
 *
 * i.e., are any members set in 'a' that are not also set in 'b'.
 */


/*
 * bms_singleton_member - return the sole integer member of set
 *
 * Raises error if |a| is not 1.
 */


/*
 * bms_get_singleton_member
 *
 * Test whether the given set is a singleton.
 * If so, set *member to the value of its sole member, and return true.
 * If not, return false, without changing *member.
 *
 * This is more convenient and faster than calling bms_membership() and then
 * bms_singleton_member(), if we don't care about distinguishing empty sets
 * from multiple-member sets.
 */


/*
 * bms_num_members - count members of set
 */
int
bms_num_members(const Bitmapset *a)
{
	int			result = 0;
	int			nwords;
	int			wordnum;

	Assert(bms_is_valid_set(a));

	if (a == NULL)
		return 0;

	nwords = a->nwords;
	wordnum = 0;
	do
	{
		bitmapword	w = a->words[wordnum];

		/* No need to count the bits in a zero word */
		if (w != 0)
			result += bmw_popcount(w);
	} while (++wordnum < nwords);
	return result;
}

/*
 * bms_membership - does a set have zero, one, or multiple members?
 *
 * This is faster than making an exact count with bms_num_members().
 */



/*
 * bms_add_member - add a specified member to set
 *
 * 'a' is recycled when possible.
 */
#ifdef REALLOCATE_BITMAPSETS
#endif

/*
 * bms_del_member - remove a specified member from set
 *
 * No error if x is not currently a member of set
 *
 * 'a' is recycled when possible.
 */
#ifdef REALLOCATE_BITMAPSETS
#endif

/*
 * bms_add_members - like bms_union, but left input is recycled when possible
 */
#ifdef REALLOCATE_BITMAPSETS
#endif
#ifdef REALLOCATE_BITMAPSETS
#endif

/*
 * bms_replace_members
 *		Remove all existing members from 'a' and repopulate the set with members
 *		from 'b', recycling 'a', when possible.
 */
#ifdef REALLOCATE_BITMAPSETS
#endif

/*
 * bms_add_range
 *		Add members in the range of 'lower' to 'upper' to the set.
 *
 * Note this could also be done by calling bms_add_member in a loop, however,
 * using this function will be faster when the range is large as we work at
 * the bitmapword level rather than at bit level.
 */
#ifdef REALLOCATE_BITMAPSETS
#endif
#ifdef REALLOCATE_BITMAPSETS
#endif

/*
 * bms_int_members - like bms_intersect, but left input is recycled when
 * possible
 */
#ifdef REALLOCATE_BITMAPSETS
#endif

/*
 * bms_del_members - delete members in 'a' that are set in 'b'.  'a' is
 * recycled when possible.
 */
#ifdef REALLOCATE_BITMAPSETS
#endif
#ifdef REALLOCATE_BITMAPSETS
#endif

/*
 * bms_join - like bms_union, but *either* input *may* be recycled
 */
#ifdef REALLOCATE_BITMAPSETS
#endif
#ifdef REALLOCATE_BITMAPSETS
#endif
#ifdef REALLOCATE_BITMAPSETS
#endif

/*
 * bms_next_member - find next member of a set
 *
 * Returns smallest member greater than "prevbit", or -2 if there is none.
 * "prevbit" must NOT be less than -1, or the behavior is unpredictable.
 *
 * This is intended as support for iterating through the members of a set.
 * The typical pattern is
 *
 *			x = -1;
 *			while ((x = bms_next_member(inputset, x)) >= 0)
 *				process member x;
 *
 * Notice that when there are no more members, we return -2, not -1 as you
 * might expect.  The rationale for that is to allow distinguishing the
 * loop-not-started state (x == -1) from the loop-completed state (x == -2).
 * It makes no difference in simple loop usage, but complex iteration logic
 * might need such an ability.
 */
int
bms_next_member(const Bitmapset *a, int prevbit)
{
	int			nwords;
	int			wordnum;
	bitmapword	mask;

	Assert(bms_is_valid_set(a));

	if (a == NULL)
		return -2;
	nwords = a->nwords;
	prevbit++;
	mask = (~(bitmapword) 0) << BITNUM(prevbit);
	for (wordnum = WORDNUM(prevbit); wordnum < nwords; wordnum++)
	{
		bitmapword	w = a->words[wordnum];

		/* ignore bits before prevbit */
		w &= mask;

		if (w != 0)
		{
			int			result;

			result = wordnum * BITS_PER_BITMAPWORD;
			result += bmw_rightmost_one_pos(w);
			return result;
		}

		/* in subsequent words, consider all bits */
		mask = (~(bitmapword) 0);
	}
	return -2;
}

/*
 * bms_prev_member - find prev member of a set
 *
 * Returns largest member less than "prevbit", or -2 if there is none.
 * "prevbit" must NOT be more than one above the highest possible bit that can
 * be set at the Bitmapset at its current size.
 *
 * To ease finding the highest set bit for the initial loop, the special
 * prevbit value of -1 can be passed to have the function find the highest
 * valued member in the set.
 *
 * This is intended as support for iterating through the members of a set in
 * reverse.  The typical pattern is
 *
 *			x = -1;
 *			while ((x = bms_prev_member(inputset, x)) >= 0)
 *				process member x;
 *
 * Notice that when there are no more members, we return -2, not -1 as you
 * might expect.  The rationale for that is to allow distinguishing the
 * loop-not-started state (x == -1) from the loop-completed state (x == -2).
 * It makes no difference in simple loop usage, but complex iteration logic
 * might need such an ability.
 */



/*
 * bms_hash_value - compute a hash key for a Bitmapset
 */


/*
 * bitmap_hash - hash function for keys that are (pointers to) Bitmapsets
 *
 * Note: don't forget to specify bitmap_match as the match function!
 */


/*
 * bitmap_match - match function to use with bitmap_hash
 */

