# MarTech Asset Bundles demo script

## Outcome

In 20 minutes, show that one reviewed source package moves from a developer
laptop to production through a governed GitHub release pipeline:

- **Dev:** FEVM, catalog `nethra`, target `dev`, deployed from the CLI as the
  developer inner loop.
- **Prod:** sandbox workspace, catalog `martech_prod`, target `prod`, deployed
  only by GitHub Actions as a service principal after PR review and an
  approved environment gate.

The central line: **we promote code, configuration, and governed object
definitions through pull requests and pipelines; we never copy environment
data forward, and no human deploys to production from a laptop.**

## Before the room joins

1. Open this project in VS Code; terminal font large.
2. Browser tabs, in order:
   - GitHub repo `db-nethra/martech-dab-demo`
   - Actions tab of the repo (with the last green `Deploy prod` run open as a
     fallback tab)
   - Dev pipeline in FEVM (open from `bundle summary -t dev -p
     fe-vm-vdm-serverless-zr8ajc`)
   - Prod pipeline (URL_PLACEHOLDER — filled after first release into the new workspace)
     — note the pipeline's creator is the `martech-dab-deployer` service
     principal, not a person
3. Confirm the local gate and dev auth:

   ```bash
   databricks auth describe --profile fe-vm-vdm-serverless-zr8ajc
   uv run pytest && uv run ruff check pipeline scripts "notebooks/Validate Promotion.py"
   ```

4. Confirm both pipelines show a successful latest update and `main` is green
   in Actions.
5. Have a release branch ready to type: the live change is bumping
   `release_label` in `databricks.yml`.

## 0:00–2:00 — Frame the operating-model question

Say:

> The question is bigger than whether a notebook runs. the MarTech team asked how code,
> pipelines, table logic, configuration, and governance move through controlled
> environments — with audit evidence, separation of duties, no secrets in
> source, and no way for an individual to change production from a laptop.
> Everything you'll see maps to one of those concerns.

Clarify the boundary once:

- Terraform / the platform team owns workspaces, networking, catalogs, and
  enterprise identity.
- The Asset Bundle owns workload assets: schemas inside existing catalogs, a
  managed Volume, serverless jobs, a Lakeflow pipeline, code, DQ rules.
- Real production data stays in production; this demo intentionally uses only
  synthetic records, generated independently inside each environment.

## 2:00–5:00 — The package: everything that promotes, in one folder

Show the tree in the VS Code explorer (mention `databricks bundle init` as the
greenfield entry point, then move on — don't run it):

```text
databricks.yml            ← bundle identity + per-target contracts
resources/data_assets.yml ← UC schemas, managed Volume, seed job
resources/pipeline.yml    ← Lakeflow pipeline + run/validate jobs
pipeline/{bronze,silver,gold}.py ← transformations + named DQ expectations
notebooks/Validate Promotion.py  ← automated acceptance gate
.github/workflows/        ← the promotion machinery itself is versioned
```

Open `databricks.yml` and pause on exactly four things:

1. One bundle identity, two target contracts: `dev` (development mode, FEVM,
   catalog `nethra`, developer-prefixed names) and `prod` (production mode,
   its own workspace, catalog `martech_prod`, stable names).
2. Prod controls are declarative: deployment lock, `fail_on_active_runs`,
   `prevent_destroy` on schemas and the Volume.
3. What's absent: no tokens, no passwords, no AWS keys anywhere in the folder.
4. The pipeline code consumes deployed resource names via substitution —
   nothing is hardcoded, so names can't drift between environments.

Say:

> The values differ by target; the source files do not. That asymmetry is the
> whole promotion model.

## 5:00–8:00 — Dev inner loop: fast, safe, disposable

In the terminal:

```bash
databricks bundle validate -t dev -p fe-vm-vdm-serverless-zr8ajc
databricks bundle deploy   -t dev -p fe-vm-vdm-serverless-zr8ajc
```

Open the dev pipeline UI: developer-prefixed resources, isolated schemas, the
developer's own synthetic data. Say:

> This is the inner loop — seconds from save to deployed, sandboxed per
> developer by development mode. What a developer can NOT do is point these
> commands at production: they hold no production credential. Promotion has
> exactly one path, and it goes through GitHub.

## 8:00–15:00 — Promotion: PR → checks → merge → approved release

The live release is a one-line change. On a branch, bump `release_label`
(e.g. `v1.1.0 → v1.1.1`) in `databricks.yml`, then push and open the PR
(VS Code Source Control or `gh pr create`).

Walk the PR while checks run (~2 min — narrate, don't wait silently):

- **Unit tests and lint** run on every PR — same gate the developer runs
  locally with `uv run pytest`.
- **Bundle validate + deployment plan** run against the prod target. Open the
  job summary: the plan is right there — one pipeline update, zero deletions —
  and it's retained as a build artifact for 90 days. Say:

  > Before anything can touch production, there is a machine-generated,
  > reviewable statement of exactly what would change, attached to the pull
  > request a human is approving. That is the audit artifact.

Merge the PR. The `Deploy prod` workflow starts — and pauses. Show the yellow
**"Waiting for review"** banner. Say:

> Merging did not deploy. The workflow is stopped at the protected prod
> environment until a required reviewer — a different role from the author in
> your setup — explicitly approves this release.

Click **Review deployments → Approve**. While the release runs (validate →
plan → deploy → pipeline refresh → acceptance gate), open the prod pipeline
UI and show the release flowing in: stable schema names, a deployment owned by
the `martech-dab-deployer` service principal (not a person), new
`release_label` in the pipeline configuration.

> No human credential can do what you just watched. The only identity that can
> touch this production workspace is a service principal that only an approved
> workflow run can exercise.

If the run is slow, pivot to the fallback known-good run tabs and keep
narrating; the workflow logs from the last successful release tell the same
story.

## 15:00–18:00 — The proof, mapped to their concerns

Open the acceptance-gate output (Actions log or the validation job run). Both
environments report the identical contract: 631/21/4,838 raw, 11/6/25
quarantined, 620 client versions, 15 campaigns, 2,966 daily facts,
`gold_direct_identifier_check = PASS`, `synthetic_data_only = true`.

> Prod did not trust dev's results — it rebuilt and re-verified its own
> environment from the same package. Data never moved.

Close the loop on the five concerns, pointing at what they just watched:

| Concern | What they saw |
|---|---|
| Auditability | Plan on the PR, retained artifacts, Actions logs, run URLs |
| Separation of duties | Developer deploys dev only; a required reviewer approves every prod release; no shared credentials |
| Secrets | Nothing in source; a service principal's OAuth secret lives in GitHub environment secrets |
| Governance | UC schemas/Volumes as reviewed declarations; PII-safe Gold enforced by an automated check |
| Unauthorized prod changes | Only the SP can write to prod, and only an approved workflow run can exercise it; deployment lock; `prevent_destroy`; every change traces to a merged PR |

## 18:00–20:00 — What the customer layers on

> This pipeline is deliberately minimal so the mechanics are visible — but the
> gate, the service principal, and the environment-scoped secret you just saw
> are the real controls, not stand-ins. Hardening to your standards is
> configuration, not redesign: code-owner review and branch protection; one
> service principal per environment; secrets in your approved identity and
> secret-management systems via OIDC; UC grants and PII-tag enforcement in the
> same reviewed YAML; and segregation-of-duties rules mapped to GitHub teams.

Invite discussion: which MarTech pipeline becomes the first real release
candidate, and who owns the release-manager role.

## Discipline during the demo

- Never run `--force`, `--force-lock`, `--auto-approve`, or `bundle destroy`.
- Never deploy `-t prod` from the laptop while the room is watching — the
  entire point is that the only prod path is the pipeline.

## Anticipated questions

- **"Where's UAT?"** — Same target shape as prod: add a `uat` target block and
  a workflow environment between dev and prod. Two environments keep this demo
  legible; the pattern is N-environment.
- **"How does configuration change between dev and prod?"** — It doesn't
  change at deploy time; it's selected. Each target carries a pre-reviewed
  variable block in `databricks.yml`; resources reference `${var.*}` and code
  reads runtime config, so source is environment-blind. A new
  environment-specific value ships as a variable with dev and prod values in
  the same PR — the reviewer approves code and both environments'
  configuration in one diff. Nobody edits settings between environments.
- **"When do we create the bundle?"** — Never as a separate step: the repo is
  the bundle. Developers write new pipelines inside an already-deployable
  project; `bundle deploy` syncs the folder and applies the declared
  resources.
- **"How do we roll back?"** — Revert the commit, PR, merge, approve the gated
  deploy. Same audited path in reverse. Data assets are protected from
  deletion throughout.
- **"Doesn't this overlap Terraform?"** — No. Terraform owns workspaces,
  catalogs, networking, identity; the bundle owns workload assets inside them.
  Different lifecycles, different owners, deliberate seam.
- **"What about grants and PII tags?"** — Deployable through the same bundle
  YAML, reviewed in the same PRs; layered on once principals and the customer's tag
  taxonomy are confirmed.
- **"Could someone deploy to prod from a laptop?"** — Only by stealing the
  service principal's secret, which lives in a protected GitHub environment
  and is only injected into approved workflow runs. Humans hold no prod
  credential; the break-glass admin path is auditable and exceptional.
