# Git / GitHub workflow (Stages 09 and 10)

This project remains in ONE existing Git repository. Do not run `git init`
again after it has been initialized, or create a separate repository per stage.

## 1. Before copying Stage 10 files: checkpoint your existing Stage 09 state

In PowerShell in your existing project directory:

```powershell
git status
git remote -v
git branch --show-current
git tag --list 'stage-*'
```

If Stage 09 has uncommitted changes AND its relevant tests have passed, commit
that *actual Stage 09 tree* before overwriting it:

```powershell
git add .
git diff --cached --check
git commit -m "feat: stage 09 controller crash recovery and stage 08 fixes"
# Tag only if stage-09 does not already exist:
git tag stage-09
git push -u origin main
git push origin stage-09
```

If the Stage 09 tests have not passed yet, keep the changes in a separate
working branch or back them up. Do NOT label untested code as an accepted
release. Do NOT create stage-08 on a Stage 09/10 commit just to fill a gap.
If a tag already exists, never force-replace it without a conscious review.

If `origin` is missing, create a GitHub repository first. Then use its actual
URL, for example:

```powershell
git remote add origin https://github.com/YOUR_USER/distributed-banking-platform.git
```

Do not publish `.env`, passwords, Docker socket tokens, DB data or private keys.
Check with `git check-ignore -v .env` and `git ls-files .env` (second should be empty).

## 2. Install Stage 10 files (keep .git and .env)

Unpack the ZIP and copy its contents into the existing project directory.
No new Git repository is necessary.

## 3. After all Stage 10 Docker acceptance tests pass

```powershell
git status
git diff --check
git check-ignore -v .env
git ls-files .env
# If .env appears in git ls-files, STOP and resolve before pushing.
git add .
git diff --cached --check
git commit -m "feat: stage 10 consistent snapshot and durable source CDC"
git push -u origin main
# Create once, after the accepted Stage 10 commit:
git tag stage-10
git push origin stage-10
git log --oneline --decorate -6
```

## GitHub Actions

`.github/workflows/python-ci.yml` validates Python syntax, runs the unit and
contract tests, and validates Docker Compose configuration for pushes and PRs.
It deliberately does not run Docker socket self-healing fault injection in the
public CI runner. Local Docker acceptance tests remain mandatory.

If you share the GitHub repository URL, the tags and workflow can be reviewed
against the live repository.
