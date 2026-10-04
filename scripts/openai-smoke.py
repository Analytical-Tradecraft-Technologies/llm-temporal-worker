#!/usr/bin/env python3
"""Opt-in, one-submission OpenAI smoke using isolated Compose dependencies."""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]


def read_key(path):
    """Read only OPENAI_API_KEY; never execute dotenv shell expressions."""
    keys = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line.startswith("export "):
            line = line[7:].lstrip()
        name, separator, value = line.partition("=")
        if separator and name.strip() == "OPENAI_API_KEY":
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            if not value or any(c.isspace() for c in value) or any(c in value for c in "`$\\"):
                raise ValueError("OPENAI_API_KEY must be a literal nonempty value")
            keys.append(value)
    if len(keys) != 1:
        raise ValueError("dotenv must contain exactly one OPENAI_API_KEY assignment")
    return keys[0]


def run(command, env, secret, timeout=240):
    try:
        result = subprocess.run(command, cwd=ROOT / "golang", env=env,
                                capture_output=True, text=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired:
        raise RuntimeError("local smoke command timed out") from None
    output = (result.stdout + result.stderr).replace(secret, "[REDACTED]")
    if result.returncode:
        if command[0] == "docker":
            # Compose never receives the provider key.
            print(output[-4000:], file=sys.stderr)
        else:
            # Expose our static assertions, never raw provider error bodies.
            for line in output.splitlines():
                if "openai_smoke_integration_test.go:" in line or line.startswith(("FAIL", "--- FAIL", "# ")):
                    print(line, file=sys.stderr)
        raise RuntimeError("local smoke command failed: " + command[0])
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    args = parser.parse_args()
    if os.environ.get("CI"):
        raise ValueError("paid smoke is operator-only")
    secret = read_key(args.env_file)
    env = dict(os.environ)
    env.pop("OPENAI_API_KEY", None)
    # Never allow an inherited Compose override to select another stack.
    for name in list(env):
        if name.startswith("COMPOSE_"):
            del env[name]
    env.update(COMPOSE_DISABLE_ENV_FILE="1", LLMTW_COMPOSE_TEMPORAL_PORT="0",
               LLMTW_COMPOSE_TEMPORAL_UI_PORT="0", LLMTW_COMPOSE_REDIS_PORT="0",
               LLMTW_REDIS_USERNAME="local", LLMTW_REDIS_PASSWORD="local-only",
               LLMTW_REDIS_KEY_PREFIX="llmtw", LLMTW_POSTGRES_PASSWORD="local-only")
    compose = ["docker", "compose", "--env-file", os.devnull, "-f", str(ROOT / "golang/compose.yaml"),
               "-p", "llmtw-openai-smoke-" + uuid.uuid4().hex[:12]]
    run(["docker", "info", "--format", "{{.ServerVersion}}"], env, secret, timeout=15)
    try:
        print("Starting isolated local Temporal and Redis", flush=True)
        run(compose + ["up", "-d", "--wait", "--wait-timeout", "180", "temporal", "redis"], env, secret)
        test_env = dict(env)
        test_env.update(OPENAI_API_KEY=secret, LLMTW_OPENAI_SMOKE="1", LLMTW_CLOUD_TEST_PROVISION="1",
                        LLMTW_TEMPORAL_ADDRESS=run(compose + ["port", "temporal", "7233"], env, secret).strip(),
                        LLMTW_REDIS_ADDR=run(compose + ["port", "redis", "6379"], env, secret).strip())
        print("Running gpt-6-luna smoke (one POST maximum, 256 output tokens)", flush=True)
        run([os.environ.get("GO", "go"), "test", "-count=1", "-timeout=4m",
             "-tags=cloudworkflowintegration,openaismoke", "./internal/runtime", "-run", "^TestLocalOpenAISmoke$"], test_env, secret, timeout=270)
        print("PASS: real generation workflow, checkpoint and cache replay")
    finally:
        run(compose + ["down", "--volumes", "--remove-orphans", "--timeout", "10"], env, secret, timeout=60)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError) as error:
        # File/command exceptions can contain user-controlled text; do not echo it.
        print("OpenAI smoke could not complete (" + type(error).__name__ + "). Check Docker, Go and the dotenv file.", file=sys.stderr)
        sys.exit(1)
