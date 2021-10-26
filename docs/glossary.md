# Glossary

## ACL-Filtering

_ACL_, or "Access Control List" is originally a term for a list of entitlements, but in its colloquial usage, is synonymous with the word "permission".
_ACL filtering_ is filtering a list of objects by whether or not someone/something has access to the items in the list.

Put another way, this is one solution to the question "What are all the things that this user has access to?".

Typically there two strategies to filtering objects: request only objects that have access or after you've received all objects check whether they have access.
Sometimes these strategies can even be combined: for example, if the set of objects is a cached result and you don't know if the cache has been invalidated, you'd want re-check all the cached objects.

## Authentication

Authentication is the act of proving or establishing a user's **identity**.
In most applications this manifests itself as "log in", and verifies to the application which "entity" (typically a user) is accessing the application.

Example pseudocode:

```py
assert verify_session(request.cookie)
user = user_from_session(request.cookie)
```

## Authorization

Authorization is the act of proving that an entity (user) has **permission** to access a resource, perform some action, or is otherwise permitted somewhere.

In most applications, authorization is handled by having inline permissions checks, where code will check the user’s permission before performing an action or returning some data.

Example pseudocode:

```py
if user.can_view(resource):
    return resource
```

## Authzed

Authzed is the company that created SpiceDB and is the primary sponsor of its development.
Their products include support contracts and a planet-scale, serverless platform for those that do not want to operate SpiceDB themselves.
They also host [documentation][docs] and operate additional tools such as a [Schema playground][playground].

[docs]: https://docs.authzed.com
[playground]: https://play.authzed.com

## Datastore

Datastores are implementations of the interface that SpiceDB uses to persist Relationship and Schema data.

## Dispatchers

Dispatchers are implementations of the interface that SpiceDB uses to evaluate requests.
The default behavior is to break down requests into subproblems that can be evaluated as parallel as possible within the same process.
When configured with information about the current deployment, SpiceDB can consistently map subproblems to other instances such that each instance's cache-hit rate increases dramatically.

## New Enemy Problem

The "New Enemy Problem" is an issue that can appear in systems where resources and permissions are distributed amongst replicated data sources, and therefore, not bounded by time in how "out of date" they can get.

Imagine a system where there are two databases, one in the United States and one in Europe, with updates being replicated from one database to the other, as changes are made.
Lets now say a user ("Alice") in the United States removes another user’s ("Bob") permission on a document, and then immediately afterwards updates the contents of the document with information that Bob cannot know about.
If the updates to the document reach the European replica of the database **before** the permission updates, then Bob could see the new information, which is caused by Bob’s permission change having arrived out-of-order.

For an more in-depth discussion of the New Enemy Problem you can read the following resources:

- [New Enemies](https://authzed.com/blog/new-enemies/) blog post

## Object

Objects represent a unique entity that is being modelled in SpiceDB.
They are composed of a type and an unique identifier and often displayed as those two values separated by a colon.
You can refer to a collection of Objects by additionally specifying one of their relations separated by a number-sign.

[Relationships](#relationship) are composed of references to objects for both the Resource and Subject.

Examples: `user:emilia`, `organization:42`, `document:a4259512-3506-4030-a2c7-4b3fd1e9d199`, `team:devops#managers`

## Policy Engine

A Policy Engine is software that process programs called _policies_ in order to produce a final decision.
Policies are expressed in _policy languages_ that vary depending on the engine.
Some languages are very limited in functionality, while others are [Turing Complete].

[Turing Complete]: https://en.wikipedia.org/wiki/Turing_completeness

In permission systems, policy engines are used to determine whether or not a _subject_ has access to perform an _action_ on an _object_.
This can occur at various points during a request's lifecycle:

- early in software infrastructure such as load-balancing
- in a reverse-proxy directly in front of an application
- after an application has received a request and gathered any additional context to be used as input

Choosing between these integration points depends on what the goal of the policies are and what input data is available at each.

### Pros & Cons

Policy Engines are great because they separate policy from the business logic in your application.
This separation allows for much easier changes to policy than if the policy is deeply coupled to the application.
A dedicated language for describing policies can also be far more succinct than a general-purpose programming language.
Policies are also often [pure], so they can be exhaustively tested.

Because Policy Engines are only focused on _checking_ access to a resource, they are not full solution for solving permissions in an application.
Policies can only evaluate input and compute "yes or no", so they do not efficiently answer questions such as "Who are all the people with access to this resource?" or "What are all the resources that this user can access?".

Policy Engines can only be as consistent as the input data they are provided, which often puts the task of synchronizing input data into the hands of the application developer.
This could be either good or bad, depending on the design of the policy engine and the requirements of the application.

[pure]: https://en.wikipedia.org/wiki/Pure_function

## Relationship

Relationships represent the existence of a live relation between an resource object and subject object or set of subject objects.
It is often displayed as these three values separated by spaces.

Examples: `document:readme writer user:emilia`, `team:devops admin organization:42#admin`

## Schema

A structural definition of the objects stored in SpiceDB, how they relate, and their permissions checks.
Relations are used to specify the types of other Objects and how they relate to the current Object.
Permissions are used to specify how to compute a permissions check with the defined relations to the current Object.

Example:

```zed
definition user {}

definition document {
	relation writer: user
	relation reader: user

	permission edit = writer
	permission view = reader + edit
}
```

## SpiceDB

A database system for managing security-critical permissions checking that was originally inspired by Google's [Zanzibar](#zanzibar) paper.
In addition to providing a database platform for storing and querying critical authorization data, SpiceDB also provides a development workflow to model, test, and validate designs for permissions systems.

## User Defined Roles

User Defined Roles is the ability to have users of an application define their own custom roles with varying levels of access.
You often see this functionality in software with admin interfaces where the access of different user classes are defined by admins.

For more information, you can read [this blog post][udr-post].

[udr-post]: https://authzed.com/blog/user-defined-roles

## Zanzibar

A global authorization service built at Google.
It was publicly documented in a [research paper][zanzibar-paper] in 2019.

For an more in-depth discussion of Zanzibar you can read the following resources:

- [What is Zanzibar?](https://authzed.com/blog/what-is-zanzibar/) blog post
- [Papers We Love NYC: Zanzibar](https://www.youtube.com/watch?v=1nbSbe3kw2U) presentation video

[zanzibar-paper]: https://research.google/pubs/pub48190/

### Notable Differences

Excluding implementation details that are vague or not described in the paper, SpiceDB has made a few notable deviations from the designs in the Zanzibar paper:

- Zanzibar represents users as numerical values that are special-cased as a distinct concept.
  SpiceDB leaves modeling users as an exercise to the schema author so that they can create more complex representations of users.

- SpiceDB supports an optional prefix for Object Types that can be used for enforcing security boundaries when multiple tenants are using the same deployment.

### Mapping Terminology from the Zanzibar paper

| Zanzibar Term      | SpiceDB Term        |
|--------------------|---------------------|
| Tuple              | Relationship        |
| Namespace          | Object Type         |
| Namespace Config   | Schema              |
| Userset            | Subject Reference   |
| User               | Subject Reference   |
| Zookie             | ZedToken            |
| Tupleset           | Relationship Set    |
| Tupleset Filter    | Relationship Filter |

## ZedToken

ZedTokens are values that represents the context under which SpiceDB will evaluate a request.
This is currently used to control data consistency, but is reserved to potentially support additional context in the future.
