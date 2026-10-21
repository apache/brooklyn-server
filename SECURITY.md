# Security Policy

This document describes how to report a security vulnerability in Apache Brooklyn.
Our security model; what the project assumes and guarantees, and which reports are
expected behavior rather than vulnerabilities, is documented separately in
[the thread model](THREAT_MODEL.md), following the
[ASF guidance on documenting a project's security model](https://cwiki.apache.org/confluence/spaces/SECURITY/pages/308153000/Documenting+your+security+model).

## Reporting a Vulnerability

Apache Brooklyn is an actively maintained ASF project. Please **do not** open a public
GitHub issue for a suspected security vulnerability. Instead, report it to
**security@apache.org**, following the process described at
<https://www.apache.org/security/>. Include:

- The affected commit/tag and, if applicable, release version
- Whether the issue requires authentication, and which entitlement(s) (see
  [the thread model](THREAT_MODEL.md)) the attacker is assumed to hold
- Whether the behavior is reachable with Apache Brooklyn's default configuration, or only
  after specific non-default configuration
- Steps to reproduce, ideally a minimal blueprint/API call

The ASF Security Team will work with the Apache Brooklyn PMC to assess and fix confirmed
issues, coordinate disclosure, and issue a CVE where warranted. See past
[Security Advisories](https://brooklyn.apache.org/community/security/) for previously
disclosed CVEs.

## Before You Report

Read [the thread model](THREAT_MODEL.md) first. Apache Brooklyn is a management/orchestration
server: authenticated users submit blueprints that Apache Brooklyn executes, including running
installation scripts and commands on the machines it manages. Much of what would be a
vulnerability in a typical web application, e.g. a privileged user running code, or a
catalog author reaching classes on the classpath, is, by design, expected behavior
here. [THREAT_MODEL.md](THREAT_MODEL.md) lists these explicitly under "[What is NOT considered a security
vulnerability](THREAT_MODEL.md#what-is-not-considered-a-security-vulnerability)", so a report that falls squarely 
within it will be closed with a reference to that section rather than treated as new.
