import argparse
import os
import re
import sys
from pathlib import Path

import snowflake.connector

def required(*names: str) -> str:
    for name in names:
        v = os.getenv(name)
        if v:
            return v

    if len(names) == 1:
        print(f"Missing env var: {names[0]}", file=sys.stderr)
    else:
        print(f"Missing env var (one of): {', '.join(names)}", file=sys.stderr)
    sys.exit(2)

def load_env_from_ps1(path: str) -> None:
    env_file = Path(path)
    if not env_file.exists():
        return

    # Supports lines like: $env:VAR_NAME="value" or $env:VAR_NAME='value'
    pattern = re.compile(r"^\s*\$env:([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(['\"])(.*?)\2\s*$")

    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        match = pattern.match(line)
        if not match:
            continue

        name, _, value = match.groups()
        os.environ.setdefault(name, value)

def get_value(row, cols, *names):
    for name in names:
        if name in cols:
            return row[cols.index(name)]
    return None

def print_repo_images(repo_fqn_display, rows, cols):
    print(f"Images in repository: {repo_fqn_display}")

    pairs = []
    for row in rows:
        img = get_value(row, cols, "image_name", "name")
        tag = get_value(row, cols, "tag")
        if img and tag:
            pairs.append((str(img), str(tag)))

    pairs = sorted(set(pairs))
    if not pairs:
        print("  (empty)")
        return

    for img, tag in pairs:
        print(f"  - {img}:{tag}")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Check or list images in a Snowflake image repository."
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all image:tag entries in the configured repository and exit.",
    )
    parser.add_argument(
        "--image",
        help="Image name to check. Defaults to IMAGE_NAME env var.",
    )
    parser.add_argument(
        "--tag",
        help="Image tag to check. Defaults to IMAGE_TAG env var (or py311).",
    )
    return parser.parse_args()

def main():
    args = parse_args()
    env_file = os.getenv("SNOWFLAKE_ENV_FILE", "snowflake_env.ps1")
    load_env_from_ps1(env_file)

    # --- Snowflake connection ---
    account  = required("SNOWFLAKE_ACCOUNT")     # ex: MLWWZGB-YR87884
    user     = required("SNOWFLAKE_USER")        # ex: JOHNPOC022026
    password = required("SNOWFLAKE_PASSWORD")
    role     = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

    # --- Repo + image info ---
    db     = required("SNOWFLAKE_DB", "POC_SPCS_DB")              # ex: POC_SPCS_DB
    schema = required("SNOWFLAKE_SCHEMA", "POC_SPCS_SCHEMA")      # ex: POC_SPCS_SCHEMA
    repo   = required("SNOWFLAKE_IMAGE_REPO", "POC_REPO")         # ex: POC_REPO
    image  = args.image or required("IMAGE_NAME")                 # ex: ds-repo-docker-custom-image
    tag    = args.tag or os.getenv("IMAGE_TAG", "py311")          # ex: py311

    repo_fqn = f'"{db}"."{schema}"."{repo}"'
    repo_fqn_display = f"{db}.{schema}.{repo}"

    ctx = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role=role,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )

    try:
        cur = ctx.cursor()
        try:
            # Must be in correct DB/SCHEMA for SHOW IMAGES if not using FQN
            cur.execute(f'USE DATABASE "{db}"')
            cur.execute(f'USE SCHEMA "{schema}"')

            # Returns image_name + tag among other columns
            cur.execute(f"SHOW IMAGES IN IMAGE REPOSITORY {repo_fqn}")
            rows = cur.fetchall()
            cols = [c[0].lower() for c in cur.description]

            if args.list:
                print_repo_images(repo_fqn_display, rows, cols)
                sys.exit(0)

            found = False
            for r in rows:
                img_name = get_value(r, cols, "image_name", "name")
                img_tag  = get_value(r, cols, "tag")
                if (img_name == image) and (img_tag == tag):
                    found = True
                    break

            if found:
                print(f"FOUND: {repo_fqn_display}/{image}:{tag}")
                sys.exit(0)
            else:
                print(f"NOT FOUND: {repo_fqn_display}/{image}:{tag}")
                print_repo_images(repo_fqn_display, rows, cols)
                sys.exit(1)

        finally:
            cur.close()
    finally:
        ctx.close()

if __name__ == "__main__":
    main()
