"""Offline checks: no Docker daemon, Temporal or provider is contacted."""
import importlib.util
import contextlib
import io
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("smoke", Path(__file__).with_name("openai-smoke.py"))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


class SmokeTests(unittest.TestCase):
    def keyfile(self, directory, text):
        path = Path(directory) / ".env"
        path.write_text(text)
        return path

    def test_dotenv_is_literal_and_ignores_other_assignments(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.keyfile(directory, '# comment\nOTHER=$(touch /tmp/not-executed)\nexport OPENAI_API_KEY="test-value"\n')
            self.assertEqual(smoke.read_key(path), "test-value")
            for text in ['OPENAI_API_KEY=$(command)', 'OPENAI_API_KEY=',
                         'OPENAI_API_KEY=a\nOPENAI_API_KEY=b', 'OTHER=x']:
                path.write_text(text)
                with self.assertRaises(ValueError):
                    smoke.read_key(path)

    def test_secret_only_enters_go_child_and_cleanup_is_scoped(self):
        calls = []
        def execute(command, **kwargs):
            calls.append((command, dict(kwargs["env"])))
            output = "127.0.0.1:12345\n" if "port" in command else "ok\n"
            return subprocess.CompletedProcess(command, 0, output, "")
        with tempfile.TemporaryDirectory() as directory:
            path = self.keyfile(directory, 'OPENAI_API_KEY=test-value\n')
            with patch.dict(smoke.os.environ, {"COMPOSE_FILE":"danger.yaml"}, clear=True), patch("sys.argv", ["smoke", "--env-file", str(path)]), patch.object(smoke.subprocess, "run", side_effect=execute):
                with contextlib.redirect_stdout(io.StringIO()):
                    smoke.main()
        go = [(command, env) for command, env in calls if "test" in command]
        self.assertEqual(len(go), 1)
        self.assertEqual(go[0][1]["OPENAI_API_KEY"], "test-value")
        for command, env in calls:
            self.assertNotIn("test-value", command)
            self.assertNotIn("COMPOSE_FILE", env)
            if command[0] == "docker":
                self.assertNotIn("OPENAI_API_KEY", env)
        down = calls[-1][0]
        self.assertIn("down", down)
        self.assertTrue(down[down.index("-p") + 1].startswith("llmtw-openai-smoke-"))

    def test_failed_provider_test_still_cleans_up(self):
        commands = []
        def execute(command, **kwargs):
            commands.append(command)
            if "test" in command:
                return subprocess.CompletedProcess(command, 1, "test-value", "secret response body")
            return subprocess.CompletedProcess(command, 0, "127.0.0.1:12345", "")
        with tempfile.TemporaryDirectory() as directory:
            path = self.keyfile(directory, 'OPENAI_API_KEY=test-value\n')
            with patch.dict(smoke.os.environ, {}, clear=True), patch("sys.argv", ["smoke", "--env-file", str(path)]), patch.object(smoke.subprocess, "run", side_effect=execute):
                with self.assertRaises(RuntimeError) as caught:
                    with contextlib.redirect_stdout(io.StringIO()):
                        smoke.main()
                self.assertNotIn("test-value", str(caught.exception))
                self.assertNotIn("secret response body", str(caught.exception))
        self.assertIn("down", commands[-1])

    def test_docker_timeout_does_not_start_stack(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.keyfile(directory, 'OPENAI_API_KEY=test-value\n')
            with patch.dict(smoke.os.environ, {}, clear=True), patch("sys.argv", ["smoke", "--env-file", str(path)]), patch.object(smoke.subprocess, "run", side_effect=subprocess.TimeoutExpired("docker", 15)) as execute:
                with self.assertRaises(RuntimeError):
                    smoke.main()
                self.assertEqual(execute.call_count, 1)


if __name__ == "__main__":
    unittest.main()
