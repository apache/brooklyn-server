# Apache Brooklyn Threat Model

This document describes Apache Brooklyn's **security model**, the trust assumptions Apache Brooklyn
makes about its users and deployment environment, so operators know what to lock down
and security researchers can distinguish a genuine vulnerability from expected,
documented behavior. See [SECURITY.md](SECURITY.md) for how to report a vulnerability.

Apache Brooklyn is a management/orchestration server: authenticated users submit
**blueprints** (YAML/Java describing applications, policies and catalog items) and
Apache Brooklyn executes them, which, by design, includes running installation scripts and
commands on the machines it manages (via SSH, WinRM, Kubernetes, and cloud provider
APIs). Keep that premise in mind throughout this document: **the capability to deploy
or extend a blueprint is, by design, close to the capability to run code**, both on the
infrastructure Apache Brooklyn manages and, for some operations, on the Apache Brooklyn server itself.

## User types / trust levels

| Role                                           | Description                                                                                                                                                                                                                                                                                                                                                                                                                        |
|------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Deployment Manager (operator)**              | Installs and configures Apache Brooklyn: chooses the `SecurityProvider`, the `EntitlementManager`, TLS, network binding, and which users/entitlements exist. Fully trusted; equivalent to root on the host running Apache Brooklyn.                                                                                                                                                                                                |
| **Root / admin entitled user**                 | An authenticated user entitled to `ROOT`, `SYSTEM_ADMIN`, `EXECUTE_SCRIPT`/`EXECUTE_GROOVY_SCRIPT`, or `SEE_ALL_SERVER_INFO`. Can run arbitrary code in the Apache Brooklyn server's own JVM (see below) and read/export full server state including persisted secrets. Treat as equivalent to shell access on the Apache Brooklyn host.                                                                                           |
| **Catalog / blueprint author**                 | An authenticated user entitled to `ADD_CATALOG_ITEM` / `DEPLOY_APPLICATION`. Can register new entity/policy/location types (including Java-backed ones, and "bean" types resolved via polymorphic Jackson deserialization against the classpath) and deploy applications that run commands on managed machines. **This is intentionally close to a code-execution privilege**. See "What is NOT considered a vulnerability" below. |
| **Authenticated, entitlement-restricted user** | A user with a narrower `EntitlementManager` grant, e.g. read-only (`SEE_ENTITY`, `SEE_SENSOR`, `SEE_CONFIG`) or scoped to specific entities/effectors. What this role can see or do depends entirely on the `EntitlementManager` the operator configures. Apache Brooklyn ships several (per-user, chained, delegating) but the default is unrestricted (see below).                                                               |
| **Unauthenticated user**                       | Exists only when the operator runs Apache Brooklyn with **no** `brooklyn.webconsole.security.*` configuration at all, the "quick start" path. In that mode every request is treated as the `root` entitlement. This is intentional for local/trial use and is documented further down; it is not a safe posture for anything but a trusted, isolated environment.                                                                  |

## Entitlements

Access to nearly every REST/API operation is checked against an `EntitlementManager`
(`core/.../mgmt/entitlement/Entitlements.java`). The permissions it can grant include,
among others:

| Entitlement                                                                    | Gates                                                                                                                                                                    |
|--------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SEE_ENTITY`, `SEE_SENSOR`, `SEE_CONFIG`, `SEE_ACTIVITY_STREAMS`               | Read visibility of entities, their sensors/config, and task/activity history                                                                                             |
| `SEE_CATALOG_ITEM`, `SEE_LOCATION`                                             | Read visibility of catalog items and locations                                                                                                                           |
| `INVOKE_EFFECTOR`, `MODIFY_ENTITY`, `RENAME_ENTITY`                            | Mutating a running entity, including invoking effectors (which may run remote commands)                                                                                  |
| `ADD_LOCATION`, `DELETE_LOCATION`                                              | Registering/removing locations (credentials for cloud accounts, SSH targets, etc.)                                                                                       |
| `ADD_POLICY`, `DELETE_POLICY`, `START_POLICY`, `STOP_POLICY`, `DELETE_ADJUNCT` | Policy/enricher/feed lifecycle                                                                                                                                           |
| `ADD_CATALOG_ITEM`, `MODIFY_CATALOG_ITEM`, `ADD_JAVA`                          | Registering new types/blueprints in the catalog; `ADD_JAVA` additionally gates catalog submissions that bundle compiled Java bytecode                                    |
| `DEPLOY_APPLICATION`                                                           | Deploying a blueprint as a running application                                                                                                                           |
| `SERVER_STATUS`                                                                | Version, HA state, up/healthy checks                                                                                                                                     |
| `SEE_ALL_SERVER_INFO`                                                          | A broad catch-all: full config dump, persistence export, reloading server propertie. Explicitly documented in code as "significantly more powerful than `SERVER_STATUS`" |
| `USAGE`, `HA_STATS`, `HA_ADMIN`, `SHUTDOWN`, `SYSTEM_ADMIN`                    | Operational/administrative endpoints, including shutting down the server                                                                                                 |
| `LOGBOOK_LOG_STORE_QUERY`                                                      | Querying the server's own logs, restricted, since logs can contain sensitive detail                                                                                      |
| `EXECUTE_SCRIPT`, `EXECUTE_GROOVY_SCRIPT`                                      | Running a script (Groovy) **directly in the Apache Brooklyn server's own JVM**, with no sandboxing                                                                       |
| `ROOT`                                                                         | Full/unrestricted access, including importing arbitrary persistence data into a running server                                                                           |

The `root()` entitlement manager (the default, see below) grants all of the above
unconditionally. Operators who need a meaningfully narrower trust boundary must
configure a different `EntitlementManager` (`brooklyn.entitlements.global`) and design
the split around this table, not assume one exists out of the box.

The built-in `PerUserEntitlementManager` also exposes coarser, named profiles on top of
the table above: `root` (full access, including the Groovy console), `user` (everything
except actions that affect the server itself, e.g. the Groovy console, shutdown, or
reading management-context config), `readonly`, and `minimal` (server stats only).m Set
per user with `brooklyn.entitlements.perUser.<user>=<profile>`. `user` is a meaningfully
safer default than `root` for most entitled users precisely because it already excludes
Groovy-console access.

## Direct code/OS-command execution on the Apache Brooklyn server itself

Some entitled operations run arbitrary code **in the Apache Brooklyn server process**, not on
a managed remote machine:

- `EXECUTE_SCRIPT` / `EXECUTE_GROOVY_SCRIPT` gate an endpoint that evaluates
  user-supplied Groovy directly via `GroovyShell`, with full JVM access (file I/O,
  reflection, `Runtime.exec`, etc.). Treat a user entitled to this the same as a user
  with a shell on the Apache Brooklyn host. Access to the Groovy console can be appropriately
  restricted by leveraging fine-grained user permissions. See the
  [entitlements configuration reference](https://brooklyn.apache.org/v/latest/ops/configuration/brooklyn_cfg.html).
- Catalog/type authors with `ADD_CATALOG_ITEM` can register "bean" types that reference
  arbitrary classes already on Apache Brooklyn's classpath by name (resolved through Jackson
  polymorphic deserialization). Jackson's own denylist of known deserialization-gadget
  classes still applies underneath this, but the design intent is for catalog authoring
  to be a trusted, code-deployment-equivalent operation, not something delegated to an
  untrusted party.
- Effectors, install scripts and workflow `shell`/`ssh` steps intentionally run commands,
  usually on the target/managed machine (SSH, WinRM, Kubernetes, cloud APIs), which is
  the entire point of an orchestration tool, but some locations (e.g. a `localhost`
  location) mean "the managed machine" is the Apache Brooklyn host itself.

## Data handling: logs, credentials, and persisted state

Apache Brooklyn redacts likely-sensitive values (`Sanitizer`, matching config/sensor keys
containing tokens like `password`, `secret`, `credential`, `private`, ...) in several
places, including before writing security configuration to logs and in some REST
responses. **This is applied at each call site, not globally**, a value stored under a
key that doesn't match one of these tokens, or logged via a path that doesn't call
`Sanitizer`, is not redacted. Treat any log output, persisted state export, or
full-config REST dump as potentially containing credentials, and restrict who can read
them (`SEE_ALL_SERVER_INFO`, `LOGBOOK_LOG_STORE_QUERY`, filesystem access to the
persistence store) accordingly. Do not assume logs are safe to hand to a
lower-privilege user by default.

Credentials do not need to reach a log or a config dump to be exposed: they are
commonly stored in cleartext in `brooklyn.cfg` or in blueprint YAML (location/cloud
credentials, database passwords, etc.) unless the operator opts into
[externalized configuration](https://brooklyn.apache.org/v/latest/ops/externalized-configuration.html).
Apache Brooklyn ships suppliers for a plain properties file and for HashiCorp Vault
(`VaultUserPassExternalConfigSupplier`, `VaultAppIdExternalConfigSupplier`,
`VaultTokenExternalConfigSupplier`); the docs themselves flag the fixed-token Vault
supplier as unsuitable for production. Absent externalized configuration, treat
`brooklyn.cfg` and any blueprint source control as part of the credential attack surface.

File-based persisted state (entity/location/policy XML) is written with file permissions
automatically set to `600` (owner read-write only); object-store persistence (S3, Swift,
Azure via jclouds) is recommended where available so the store's own access controls
apply. Either way, the operator is responsible for the permissions of the containing
directories, mount points and disks, Apache Brooklyn does not harden those.

## Network exposure and transport

- **Bind address**: when Apache Brooklyn is started with no security configuration at all (the
  "quick start" default), the web console binds to **all interfaces (`0.0.0.0`)**, not
  just loopback. Loopback-only binding is a separate, non-default flag.
- **TLS**: HTTPS is **off by default** (`BrooklynWebConfig.HTTPS_REQUIRED` defaults to
  `false`); the console serves plain HTTP unless explicitly configured otherwise.
- **Authentication**: when zero `brooklyn.webconsole.security.*` keys are configured, the
  effective security provider is `AnyoneSecurityProvider` (no authentication at all), and
  the effective entitlement manager is `root` (unrestricted). This combination. bound to
  all interfaces, plain HTTP, no auth, full entitlement, is the literal zero-config
  starting point, intended for a trial on a trusted machine/network, not for anything
  internet-facing.
- **Karaf management console**: Apache Brooklyn runs on Apache Karaf, which ships its own SSH
  management console, bound to localhost only by default but protected only by Karaf's
  stock default credentials until an operator changes them. Treat an unchanged Karaf
  console the same as the web console's zero-config default: localhost-only binding
  limits *remote* exposure, but it is still a default-credential admin shell on the
  Apache Brooklyn host, reachable by any local user/process. See the
  [production installation guide](https://brooklyn.apache.org/v/latest/ops/production-installation.html#configuring-karaf-security).

## Authentication behavior

- **Username enumeration**: the built-in `ExplicitUsersSecurityProvider` returns the same
  failure for an unknown username and for a known username with the wrong password; the
  distinction is only logged server-side at debug level, not surfaced to the client.
- **Brute-force protection**: Apache Brooklyn does **not** implement rate-limiting or account
  lockout on login attempts. This is left to the deployment environment (reverse proxy,
  fail2ban-style tooling, network ACLs, etc.).

## What is NOT considered a security vulnerability

Given the trust model above, the following are **expected behavior**, not
vulnerabilities, when reported without additional context:

- An authenticated user entitled to `ADD_CATALOG_ITEM`, `DEPLOY_APPLICATION`,
  `EXECUTE_SCRIPT`, `EXECUTE_GROOVY_SCRIPT`, or `ROOT` being able to run arbitrary code,
  on the Apache Brooklyn server, or on machines Apache Brooklyn manages. These entitlements are, by
  design, equivalent to a code-deployment or shell-access privilege. The bug report we
  want is "entitlement X unexpectedly grants capability Y that isn't documented here or
  in the entitlement's own description", not "a user entitled to run scripts can run
  scripts."
- Apache Brooklyn running with no authentication and full (`root`) entitlement when started with
  **zero** `brooklyn.webconsole.security.*` configuration. This is a documented,
  intentional "quick start" default for local/trial use. See "[Responsibilities of
  Deployment Managers](#responsibilities-of-deployment-managers)" below for what an operator must configure before
  exposing Apache Brooklyn beyond a trusted network.
- A blueprint, catalog "bean" type, or workflow step doing exactly what its author (an
  entitled `ADD_CATALOG_ITEM`/`DEPLOY_APPLICATION` user) told it to do, including
  fetching a URL, reading a file, or instantiating an on-classpath class the author named
  explicitly. The trust boundary is at entitlement, not inside blueprint content.
- Lack of built-in brute-force/lockout protection on login: this is an explicit
  operator/infrastructure responsibility, not a gap in Apache Brooklyn itself.
- A REST endpoint fetching a resource **only** from `classpath:` URLs or local,
  protocol-less paths ending in a recognized image extension (the icon-serving
  endpoints): these deliberately never make a server-side request for any URL bearing an
  external protocol (`http:`, `https:`, `ftp:`, `file:`, etc.); such URLs are returned to
  the client as a redirect instead. A report claiming server-side fetch of an arbitrary
  attacker-supplied URL through these specific endpoints should include a reproduction
  that demonstrates the guard (`isUrlServerSideAndSafe`) being bypassed, not just a read
  of the underlying utility method in isolation.

If you believe one of the above actually crosses a trust boundary this document doesn't
account for (e.g. a lower-privilege entitlement than the ones named above reaching one of
these capabilities), that **is** worth reporting, please say explicitly which
entitlement you tested with.

## Responsibilities of Deployment Managers

Before exposing an Apache Brooklyn instance beyond a single trusted machine/network, an operator
should, at minimum:

1. **Configure real authentication.** Set `brooklyn.webconsole.security.provider` to a
   real provider (e.g. `ExplicitUsersSecurityProvider`) with strong, unique credentials.
   Never rely on the zero-config `AnyoneSecurityProvider` default outside a trial on a
   trusted host.
2. **Restrict entitlements.** Set `brooklyn.entitlements.global` away from the default
   `root` manager, and design who gets `ADD_CATALOG_ITEM`, `DEPLOY_APPLICATION`,
   `EXECUTE_SCRIPT`/`EXECUTE_GROOVY_SCRIPT`, `ROOT`, and `SEE_ALL_SERVER_INFO`, treating
   each as equivalent to a code-execution or credential-read grant, per the table above.
3. **Enable TLS** and put the console behind it, or behind a TLS-terminating reverse
   proxy; HTTPS is not on by default.
4. **Bind to the right interface.** Don't leave the console on `0.0.0.0` if it doesn't
   need to be reachable from other hosts.
5. **Add brute-force protection at the infrastructure layer** (reverse proxy rate
   limiting, fail2ban, network ACLs), Apache Brooklyn does not provide this itself.
6. **Treat logs, persistence exports, and full-config dumps as potentially sensitive.**
   Sanitization is best-effort and name-pattern based, not a guarantee; restrict
   `SEE_ALL_SERVER_INFO` and `LOGBOOK_LOG_STORE_QUERY` and control who can read the
   persistence store on disk.
7. **Treat every catalog/blueprint author as a code-deployment-equivalent trust level.**
   Do not grant `ADD_CATALOG_ITEM` or `DEPLOY_APPLICATION` to a user or automation you
   would not trust to run arbitrary commands on Apache Brooklyn-managed infrastructure.
8. **Don't run the Apache Brooklyn server process with superuser (root) privileges.** In line
   with Apache Brooklyn's own
   [security guidelines](https://brooklyn.apache.org/v/latest/ops/security-guidelines.html),
   run Apache Brooklyn as an unprivileged OS user; this limits the blast radius of the
   JVM-level code execution described above, even for an entitled user.
9. **Change the default Karaf SSH console credentials** before relying on localhost-only
   binding as a trust boundary for that console.
10. **Prefer LDAP** (or another `SecurityProvider` backed by your real identity system)
    over the built-in username/password list for anything beyond a trial.
11. **Externalize credentials.** Use externalized configuration, ideally backed by Vault
    or an equivalent secrets manager, for cloud/location credentials and any secret
    blueprint config, rather than plaintext in `brooklyn.cfg` or YAML kept in source
    control.
12. **Harden the infrastructure Apache Brooklyn manages, not just Apache Brooklyn itself.** Scope cloud
    credentials to Apache Brooklyn's actual needs (least-privilege IAM), avoid hard-coded VM
    image passwords (use `loginUser/loginUser.password` or `loginUser.privateKeyData` and
    disable root login on boot), prefer SSH keys over passwords for managed-machine
    access, and point install-artifact downloads (RPM repos, install scripts) at
    repositories you trust, Apache Brooklyn will execute whatever they return.
