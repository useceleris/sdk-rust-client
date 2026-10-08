# Security policy

## Report a vulnerability

**Do not open a public issue for a security problem.**

Report it privately through GitHub's [Security Advisories](https://github.com/useceleris/sdk-rust-client/security/advisories/new) on this repository. That channel is private between you and the maintainer, so we can prepare a fix before anything is disclosed.

Include what is necessary to reproduce the problem: the affected version, the Rust version, and the smallest example that shows it. A suggested fix is welcome but not necessary.

You get an acknowledgement in a few days. We tell you what we found, what we will do, and when we expect a fix. We credit you when the fix is published, unless you tell us not to.

## Supported versions

Fixes go into the latest release. There is no long-term support branch.

## Scope

In scope: anything in this crate that lets one connection read, write or act as another beyond what its credentials grant, any leak of a credential to a place that it must not get to, and any input from the network that can crash or corrupt an application that uses the crate.

Out of scope: the Celeris service itself, which you report through the same channel on its own repository, and findings that need an attacker who already holds the signing secret. That secret is the trust boundary, and its compromise is total by design.

## What this crate promises

This is the client crate. It **never signs anything**: it has no signing facility and no constructor that takes a secret, and it does not depend on `useceleris-server`. A test pins its runtime dependencies on every run.

The crate treats bytes from the network as untrusted. The decoder has limits on fragment count and depth, identifiers are validated, and no server text or credential ever goes into an SDK error. `Debug` shows `Credentials` as redacted. The crate logs nothing, and it never passes on a dial error, because a dial error quotes the handshake URL, which carries the credentials. TLS certificates are verified against the system's trust store, with the crate's own `rustls` configuration, so the TLS settings of an application cannot make this connection weaker.
