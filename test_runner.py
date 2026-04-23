import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app import runner


class RunAgentPromptFileTests(unittest.TestCase):
    def test_run_agent_uses_prompt_file_instead_of_raw_argv(self):
        captured = {}

        async def fake_create_subprocess_exec(*args, **kwargs):
            captured["args"] = list(args)
            prompt_arg = args[-1]
            self.assertTrue(prompt_arg.startswith("@"))
            prompt_path = prompt_arg[1:]
            self.assertTrue(os.path.isfile(prompt_path))
            with open(prompt_path, "r", encoding="utf-8") as fh:
                captured["prompt_text"] = fh.read()

            class _FakeStream:
                async def read(self, _n=-1):
                    return b""

            class _FakeProc:
                stdout = _FakeStream()
                stderr = _FakeStream()
                returncode = 0

                async def wait(self):
                    return 0

                def terminate(self):
                    return None

                def kill(self):
                    return None

            return _FakeProc()

        long_prompt = "# Task\n\n" + "\n".join(
            f"{idx}. /very/long/path/to/file_{idx}.c" for idx in range(5000)
        )

        with tempfile.TemporaryDirectory() as cwd:
            with patch.object(runner, "_find_pi_command", return_value=["/usr/bin/pi"]):
                with patch("app.runner.asyncio.create_subprocess_exec", side_effect=fake_create_subprocess_exec):
                    result = asyncio.run(
                        runner.run_agent(
                            long_prompt,
                            model="test-model",
                            tools=["read"],
                            cwd=cwd,
                            max_retries=0,
                            pi_max_retries=0,
                        )
                    )

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(captured["prompt_text"], long_prompt)
        self.assertNotIn(long_prompt, captured["args"])


if __name__ == "__main__":
    unittest.main()
