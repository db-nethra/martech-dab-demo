# MarTech Asset Bundles operator runbook

## Environment map

| Target | Workspace | Deployed by | Catalog | Mode |
|---|---|---|---|---|
| `dev` | FEVM `2578201192173249` | CLI, profile `fe-vm-vdm-serverless-zr8ajc` | `nethra` | development |
| `prod` | sandbox `h6k0yr` | GitHub Actions, service principal `martech-dab-deployer` | `martech_prod` | production |

Repo: `db-nethra/martech-dab-demo` (public; contains no credentials or
customer references).
The bundle name, target, and workspace together identify deployment state. Do
not rename them before intentionally retiring a deployment.

## Authentication

Local profiles:

```bash
databricks auth describe --profile fe-vm-vdm-serverless-zr8ajc  # dev
databricks auth describe --profile fe-sandbox-prod              # prod (break-glass only)
```

If a profile's OAuth expires:

```bash
databricks auth login --host <workspace-url> --profile <profile>
```

CI: GitHub Actions authenticates to the prod workspace as the
`martech-dab-deployer` service principal via OAuth M2M —
`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` GitHub secrets (repo
level for PR checks, `prod` environment level for deploys). The SP owns
`ALL PRIVILEGES` on `martech_prod` and nothing else.

Never add OAuth tokens, PATs, passwords, or AWS keys to the bundle or repo.

## Local release gate

```bash
databricks version   # 0.298.0 or newer
uv run pytest        # 11 passing
uv run ruff check pipeline scripts "notebooks/Validate Promotion.py"
```

## Dev release (developer inner loop, CLI)

```bash
databricks bundle validate -t dev -p fe-vm-vdm-serverless-zr8ajc
databricks bundle plan     -t dev -p fe-vm-vdm-serverless-zr8ajc
databricks bundle deploy   -t dev -p fe-vm-vdm-serverless-zr8ajc
databricks bundle run      -t dev -p fe-vm-vdm-serverless-zr8ajc seed_landing_data
databricks bundle run      -t dev -p fe-vm-vdm-serverless-zr8ajc martech_demo_refresh
databricks bundle run      -t dev -p fe-vm-vdm-serverless-zr8ajc validate_promotion
```

The seed job uses `--clean` only inside the bundle-owned synthetic landing
Volume. It does not touch the existing workshop demo schemas.

## Prod release (GitHub Actions only)

1. Branch, change, push, open PR against `main`.
2. `PR checks` must pass: unit tests + lint, `bundle validate -t prod`, and a
   deployment plan posted to the job summary and retained as an artifact.
   Review the plan — stop on any unexpected replacement or deletion.
3. Merge. The `Deploy prod` workflow starts and **pauses at the protected
   `prod` environment** until a required reviewer approves it in the Actions
   UI ("Review deployments" → approve).
4. On approval, the workflow validates, records the plan, deploys as the
   service principal, runs the full pipeline refresh, and runs the acceptance
   gate.

Do not deploy `-t prod` from a laptop except break-glass, and record why.

Prod has a deployment lock, fails deployment when an active run could be
interrupted, and protects its schemas and Volume with `prevent_destroy`.

## Acceptance contract

Both targets must report:

| Asset | Expected rows |
|---|---:|
| `client_profiles_raw` | 631 |
| `campaigns_raw` | 21 |
| `engagement_events_raw` | 4,838 |
| `client_profiles_quarantine` | 11 |
| `campaigns_quarantine` | 6 |
| `engagement_events_quarantine` | 25 |
| `dim_client` | 620 |
| `dim_campaign` | 15 |
| `fact_engagement_daily` | 2,966 |

The validation output must also contain `status = PASS`,
`synthetic_data_only = true`, and `gold_direct_identifier_check = PASS`.

## Safe rollback and recovery

- If `validate` fails, fix configuration before any deploy.
- If a plan shows an unexpected delete or replacement, stop. Do not approve it.
- If the pipeline fails, use the run URL, correct code/configuration, and take
  the fix through a PR like any other change.
- Roll back by reverting the offending commit and releasing through the same
  PR → merge → approval path.
- Do not run `bundle destroy -t prod`; prod UC assets are intentionally
  protected.
- Do not use `--force`, `--force-lock`, or `--auto-approve` to bypass a gate.
- If a CI run fails with 401/403, verify the SP secret is current
  (`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` in repo and `prod`
  environment secrets).

## Production hardening for a customer org

Already demonstrated live: protected `prod` environment with a required
reviewer, environment-scoped secrets, a least-privileged deployment service
principal, and retained plan artifacts. Remaining hardening:

1. Branch protection + code-owner review on `main` (needs GitHub
   Team/Enterprise for private repos).
2. Secrets move to the approved identity/secret system (e.g., AWS Secrets
   Manager), surfaced to Actions via OIDC — no long-lived static secrets.
3. One service principal per environment; workspace paths move from user
   folders to an ACL-controlled deployment-identity path.
4. Persist deploy logs, job run URLs, and validation JSON alongside the plan
   artifact as release evidence.
5. Add customer group grants, PII classification/tag enforcement, and
   segregation-of-duties checks to the same reviewed YAML.
6. Keep workspace/catalog infrastructure in Terraform; keep workload promotion
   in the Asset Bundle.
