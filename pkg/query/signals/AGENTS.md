# Writing signal READMEs

Each signal package has its own `README.md`. Follow the structure below and the
existing [context-field stability](contextfield/README.md),
[result size](resultsize/README.md), [historical result size](historicalresultsize/README.md),
and [branch outcomes](branchoutcome/README.md) examples.

## Required layout

```markdown
# Human-readable signal name

## Summary

One short sentence describing what the signal tracks.

- **Revision scope:** Exact datastore revision, or across datastore revisions.
- **Aging:** Whether evidence ages; include the default decay and retention when applicable.

## Description

Explain the signal's purpose and how it can help an optimization, in plain language.

## Example

Show a summarized schema, a summarized request, and scenarios describing what the
signal learns or stores.

## Details

Document the provider-specific technical behavior and limits.
```

## Writing conventions

- Keep the Summary to one quick sentence and use the same field names above in
  every signal README. Make the revision scope explicit; do not imply that the
  signal chooses or quantizes datastore revisions.
- Write the Description for someone learning what the signal is useful for.
  Prefer concrete explanations over implementation terminology. Describe the
  general behavior first; a named example such as `region` must not make a signal
  that handles arbitrary context fields sound specific to that field.
- Put a Zed schema block and request alongside the examples. A scenario table
  should explain normal observations, incomplete or missing observations, and
  relevant changes in revision, time, or schema. State when rows start with an
  empty history instead of implying that independent scenarios accumulate.
- Keep explicit technical details in the final Details section: population keys,
  observation eligibility, estimate fields, weighting, defaults, configuration,
  bounds, schema compatibility, and unusual cases. Include the details that are
  relevant to this provider rather than repeating every topic mechanically.
- Distinguish predictions from permission proofs, incomplete work from measured
  zeroes, and a lack of evidence from a negative result. Describe current behavior;
  do not imply that a signal already drives an optimization when that wiring is
  still a subsequent change.
- Link to related signals where they are introduced. In particular, a historical
  or derived signal should link to the README of the signal it builds on near the
  start of its Description.
- Shared registration and access conventions belong in the
  [parent README](README.md). Do not repeat boilerplate such as "registered through
  the explicit signal catalog" or "access with typed Ref/Input values" in each
  signal README.
