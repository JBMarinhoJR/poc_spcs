# GitHub Actions + Snowflake Image Repository

This repository now has a workflow at:

`/.github/workflows/build-push-snowflake.yml`

It builds Docker images from `Dockerfile.multi` and pushes tags (`py311`, `py312`, `py313`, `py314`) to your Snowflake image repository.

## 1. Create or make the GitHub repository public

If the repo already exists on GitHub:

1. Go to `Settings` -> `General` -> `Danger Zone`.
2. Select `Change repository visibility`.
3. Set it to `Public`.

If you still need to push this local code to GitHub:

```powershell
git remote add github https://github.com/<owner>/<repo>.git
git push -u github main
```

## 2. Configure GitHub Secrets

In GitHub: `Settings` -> `Secrets and variables` -> `Actions` -> `New repository secret`.

Create these secrets:

- `SNOWFLAKE_ACCOUNT` (example: `MLWWZGB-YR87884`)
- `SNOWFLAKE_USER` (example: `JOHNPOC022026`)
- `SNOWFLAKE_PASSWORD` (real password)
- `SNOWFLAKE_ROLE` (example: `ACCOUNTADMIN`)
- `SNOWFLAKE_DB` (example: `POC_SPCS_DB`)
- `SNOWFLAKE_SCHEMA` (example: `POC_SPCS_SCHEMA`)
- `SNOWFLAKE_IMAGE_REPO` (example: `POC_REPO`)
- `IMAGE_NAME` (example: `ds-repo-docker-custom-image`)

## 3. Run the workflow

The workflow runs automatically on push to `main`.

You can also run manually:

1. Go to `Actions`.
2. Open `Build and Push Snowflake Image`.
3. Click `Run workflow`.

## 4. Validate images in Snowflake

Use your local script:

```powershell
python test.py --list
```

## Notes

- `snowflake_env.ps1` is now ignored by Git via `.gitignore` to avoid accidental credential leaks.
