# Security Policy

This project operates under a security embargo program.
This means that vulnerabilities are privately reported, analyzed for applicability, notice is given, and a resolution is created and distributed.
The issue is only made public once those affected in the embargo program have enough time to address the issue or have accepted the risks.

If you discover a vulnerability, avoid posting it publicly.
Please report it by sending an email to <security@authzed.com>.

For more details on how to report, see <https://authzed.com/docs/authzed/concepts/security-embargo>

If you'd like to be included in the security embargo because this project is critical to your business, consider purchasing a commercially supported version of SpiceDB.

## Threat Model

Even if you are not reporting a vulnerability, please take your time to read and understand this section carefully.
This describes the scope of responsibility of this project and importantly what it is *not* responsible for.

### Logic Errors

SpiceDB is a centralized authorization service: it stores relationships and a schema, then evaluates permission checks against them.
Because services leverage SpiceDB's API responses for security, a bug that produces an incorrect response is treated as a vulnerability.
This increases the number of vulnerabilities documented against this codebase, but is the most responsible stance for a security policy.

### Deployment Assumptions

SpiceDB is designed to be ran in one type of environment: a trusted network with trusted actors.
Preventing tampering, information disclosure, and denial of service within this type of environment is the sole responsibility of those that operate the environment.

Threats against SpiceDB are only valid if they can impact deployments with these assumptions:

- SpiceDB is the latest version of the software
- SpiceDB was acquired from an official source and its integrity has been verified
- SpiceDB is configured to use a unique, strong, and secret preshared key
- Secrets are provided to SpiceDB in a secure fashion
- SpiceDB is bound to a trusted network that is segregated from untrusted actors
- SpiceDB nodes participating within the cluster are all trusted
- SpiceDB is configured to exclusively use TLS for all network traffic
- SpiceDB's datastore is only accessible to SpiceDB
- Untrusted input is never used as input into the SpiceDB API
- SpiceDB API output is sanitized before exposure to untrusted actors
