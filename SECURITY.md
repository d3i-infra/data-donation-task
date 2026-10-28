# Security policy

This repository is the D3I fork of Eyra's [Feldspar](https://github.com/eyra/feldspar)
data-donation task. It is maintained by the D3I team (Digital Data Donation
Infrastructure, a service of ODISSEI) and runs in the browsers of research participants
who donate data to studies at Dutch universities.

## Security maintainer

Danielle McCool (Utrecht University) is the security maintainer for the d3i-infra
forks. She is responsible for triaging reports, assessing impact on deployed studies,
and coordinating fixes and disclosure with upstream where a finding also affects Eyra.

## Reporting a vulnerability

Please do not open a public issue for a security problem.

- Preferred: use GitHub's private vulnerability reporting on this repository
  (**Security → Report a vulnerability**).
- Alternative: email [DataDonation@uu.nl](mailto:DataDonation@uu.nl) with "security" in
  the subject line.

Include what you can of the following:

- the kind of vulnerability and the impact you believe it has;
- the affected file, branch, or deployed component;
- steps to reproduce, including any setup or configuration needed;
- whether participant data could have been exposed, and under what conditions.

You will receive an acknowledgment within five working days. We will keep you
informed as the report is assessed and fixed, and credit you in the fix if you wish.

## Scope

In scope:

- this repository and the other d3i-infra repositories that make up the D3I
  data-donation platform, in particular [d3i-infra/mono](https://github.com/d3i-infra/mono);
- the D3I deployments operated on SURF Research Cloud for Dutch university studies.

Out of scope here:

- findings that only affect upstream Eyra code and not the d3i-infra forks: report
  those to Eyra at security@eyra.co (see Eyra's own security policy);
- studies run by other organisations on their own forks or deployments.

## Supported versions

Fixes are made on the main branch and rolled into the currently deployed images. Older
releases are not patched separately.
