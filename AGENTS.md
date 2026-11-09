# Testing policy

Repository-level E2E testing is not required in `matrix-rust-sdk` (SDK exemption).
For SDK changes supporting a downstream client, the product E2E journey runs in
the consuming client repository.

Existing unit, integration, lint and formatting checks remain required.

Where agent E2E applies, it is required only when initially adding a feature;
future pushes or deployments do not require rerunning E2E.
