import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.agent_process import AgentProcessHandle
from app import agent_process
from app import runner


def _overflow_result() -> runner.AgentResult:
    result = runner.AgentResult()
    result.exit_code = 1
    result.error = (
        "400 litellm.BadRequestError: Hosted_vllmException - "
        '{"error":{"message":"You passed 147421 input tokens and requested 16384 output tokens. '
        "However, the model's context length is only 163804 tokens, resulting in a maximum input "
        'length of 147420 tokens. Please reduce the length of the input prompt."}}'
    )
    return result


class RunAgentPromptFileTests(unittest.TestCase):
    def test_cleanup_orphan_pi_processes_preserves_active_runtime_snapshot(self):
        orphan = agent_process.AgentProcessInfo(
            pid=101,
            ppid=1,
            pgid=201,
            comm="pi",
            exe="node",
            cwd="/tmp/dfa-task/run",
            cmdline="pi --mode rpc",
            environ={"DFA_TASK_ID": "dfa_1", "DFA_TASK_ROOT": "/tmp/dfa-task", "DFA_TASK_RUN_ROOT": "/tmp/dfa-task/run", "DFA_EXECUTION_EPOCH": "3"},
        )
        with patch.object(agent_process, "_iter_agent_processes", return_value=[orphan]):
            with patch.object(agent_process, "_kill_process_group", return_value=True) as kill_group:
                killed = agent_process.cleanup_orphan_pi_processes(
                    lambda _: None,
                    label="test",
                    owner_id="pod-a",
                    runtime_snapshots=[{
                        "task_id": "dfa_1",
                        "owner_id": "pod-a",
                        "execution_epoch": 3,
                        "status": "running",
                        "run_root": "/tmp/dfa-task/run",
                        "active_context": True,
                    }],
                )
        self.assertEqual(killed, 0)
        kill_group.assert_not_called()

    def test_cleanup_orphan_pi_processes_kills_confirmed_orphan_after_guard_rounds(self):
        orphan = agent_process.AgentProcessInfo(
            pid=101,
            ppid=1,
            pgid=201,
            comm="pi",
            exe="node",
            cwd="/tmp/dfa-task/run",
            cmdline="pi --mode rpc",
            environ={"DFA_TASK_ID": "dfa_1", "DFA_TASK_ROOT": "/tmp/dfa-task", "DFA_TASK_RUN_ROOT": "/tmp/dfa-task/run"},
        )
        tracker: dict[str, int] = {}
        with patch.object(agent_process, "_iter_agent_processes", return_value=[orphan]), patch.object(
            agent_process,
            "_task_process_started_at",
            return_value=0.0,
        ), patch.object(
            agent_process,
            "_session_activity_mtime",
            return_value=None,
        ), patch.dict("os.environ", {
            "DFA_AGENT_ORPHAN_CONFIRM_ROUNDS": "2",
            "DFA_AGENT_ORPHAN_MIN_AGE_SECONDS": "0",
        }, clear=False):
            with patch.object(agent_process, "_kill_process_group", return_value=True) as kill_group:
                first = agent_process.cleanup_orphan_pi_processes(lambda _: None, label="test", state_tracker=tracker)
                second = agent_process.cleanup_orphan_pi_processes(lambda _: None, label="test", state_tracker=tracker)
        self.assertEqual(first, 0)
        self.assertEqual(second, 1)
        kill_group.assert_called_once()

    def test_cleanup_orphan_pi_processes_respects_default_900s_min_age(self):
        orphan = agent_process.AgentProcessInfo(
            pid=101,
            ppid=1,
            pgid=201,
            comm="pi",
            exe="node",
            cwd="/tmp/dfa-task/run",
            cmdline="pi --mode rpc",
            environ={"DFA_TASK_ID": "dfa_1", "DFA_TASK_ROOT": "/tmp/dfa-task", "DFA_TASK_RUN_ROOT": "/tmp/dfa-task/run"},
        )
        with patch.object(agent_process, "_iter_agent_processes", return_value=[orphan]), patch.object(
            agent_process,
            "_task_process_started_at",
            return_value=1000.0,
        ), patch.object(
            agent_process,
            "_session_activity_mtime",
            return_value=None,
        ), patch.object(
            agent_process.time,
            "time",
            return_value=1500.0,
        ):
            with patch.object(agent_process, "_kill_process_group", return_value=True) as kill_group:
                killed = agent_process.cleanup_orphan_pi_processes(lambda _: None, label="test", state_tracker={})
        self.assertEqual(killed, 0)
        kill_group.assert_not_called()

    def test_cleanup_task_agent_processes_only_hits_matching_task(self):
        match = agent_process.AgentProcessInfo(
            pid=101,
            ppid=55,
            pgid=201,
            comm="pi",
            exe="node",
            cwd="/tmp/dfa-task/run/epochs/0001",
            cmdline="pi --mode rpc",
            environ={"DFA_TASK_ID": "dfa_match", "DFA_TASK_ROOT": "/tmp/dfa-task"},
        )
        other = agent_process.AgentProcessInfo(
            pid=102,
            ppid=1,
            pgid=202,
            comm="pi",
            exe="node",
            cwd="/tmp/other/run",
            cmdline="pi --mode rpc",
            environ={"DFA_TASK_ID": "dfa_other", "DFA_TASK_ROOT": "/tmp/other"},
        )
        with patch.object(agent_process, "_iter_agent_processes", return_value=[match, other]):
            with patch.object(agent_process, "_kill_process_group", return_value=True) as kill_group:
                killed = agent_process.cleanup_task_agent_processes(
                    lambda _: None,
                    label="test",
                    task_id="dfa_match",
                    task_root="/tmp/dfa-task",
                    run_root="/tmp/dfa-task/run/epochs/0001",
                )
        self.assertEqual(killed, 1)
        kill_group.assert_called_once()

    def test_agent_process_terminate_tree_force_cleans_group_after_exit(self):
        logs: list[str] = []

        class FakeProc:
            pid = 123
            returncode = 0

            async def wait(self):
                return 0

        async def scenario():
            with patch("app.agent_process.process_group_exists", return_value=True):
                with patch("app.agent_process.os.killpg") as killpg:
                    handle = AgentProcessHandle(
                        proc=FakeProc(),
                        label="test",
                        logger=logs.append,
                        pgid=456,
                    )
                    await handle.terminate_tree(reason="cleanup")
                    killpg.assert_called_once()

        asyncio.run(scenario())
        self.assertTrue(any("cleaning leaked pi process group" in msg for msg in logs))

    def test_sleep_with_cancel_stops_early_when_cancelled(self):
        async def scenario():
            cancel_event = asyncio.Event()

            async def trigger_cancel():
                await asyncio.sleep(0.01)
                cancel_event.set()

            asyncio.create_task(trigger_cancel())
            return await runner._sleep_with_cancel(5, cancel_event)

        completed = asyncio.run(scenario())
        self.assertFalse(completed)

    def test_pi_retry_backoff_exits_when_cancelled(self):
        async def scenario():
            cancel_event = asyncio.Event()

            async def trigger_cancel():
                await asyncio.sleep(0.01)
                cancel_event.set()

            asyncio.create_task(trigger_cancel())
            return await runner._run_with_pi_retry(
                args=["/usr/bin/pi"],
                cwd=".",
                env=None,
                prompt="hello",
                post_skill_prompt=None,
                cancel_event=cancel_event,
                on_stream=None,
                max_retries=0,
                retry_delay=0,
                pi_max_retries=-1,
                pi_retry_delay=5,
            )

        with patch.object(
            runner,
            "_run_with_api_retry",
            side_effect=runner._PiProcessError("exit_code=-9: killed"),
        ):
            result = asyncio.run(scenario())

        self.assertIn("cancelled during pi retry backoff", result.error or "")

    def test_run_agent_returns_before_spawn_when_cancelled(self):
        async def scenario():
            cancel_event = asyncio.Event()
            cancel_event.set()
            return await runner.run_agent(
                "hello",
                model="test-model",
                tools=["read"],
                cwd=".",
                cancel_event=cancel_event,
            )

        with patch.object(runner, "_find_pi_command") as find_pi_command:
            with patch.object(runner, "_run_with_pi_retry") as run_with_pi_retry:
                result = asyncio.run(scenario())

        find_pi_command.assert_not_called()
        run_with_pi_retry.assert_not_called()
        self.assertEqual(result.error, "cancelled")
        self.assertEqual(result.exit_code, -1)

    def test_run_agent_uses_prompt_file_instead_of_raw_argv(self):
        captured = {}

        async def fake_run_with_pi_retry(**kwargs):
            captured["args"] = kwargs["args"]
            captured["prompt_text"] = kwargs["prompt"]
            captured["env"] = kwargs["env"]
            result = runner.AgentResult()
            result.output = "ok"
            result.exit_code = 0
            return result

        long_prompt = "# Task\n\n" + "\n".join(
            f"{idx}. /very/long/path/to/file_{idx}.c" for idx in range(5000)
        )

        with tempfile.TemporaryDirectory() as cwd:
            with patch.object(runner, "_find_pi_command", return_value=["/usr/bin/pi"]):
                with patch.object(runner, "_run_with_pi_retry", side_effect=fake_run_with_pi_retry):
                    result = asyncio.run(
                        runner.run_agent(
                            long_prompt,
                            model="test-model",
                            tools=["read"],
                            cwd=cwd,
                            max_retries=0,
                            pi_max_retries=0,
                            task_context={
                                "task_id": "dfa_123",
                                "task_root": "/tmp/dfa_123",
                                "task_run_root": "/tmp/dfa_123/run/epochs/0001",
                                "worker_id": "worker-a",
                                "execution_epoch": 1,
                            },
                        )
                    )

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(captured["prompt_text"], long_prompt)
        self.assertNotIn(long_prompt, captured["args"])
        self.assertEqual(captured["env"]["DFA_TASK_ID"], "dfa_123")
        self.assertEqual(captured["env"]["DFA_TASK_ROOT"], "/tmp/dfa_123")
        self.assertEqual(captured["env"]["DFA_TASK_RUN_ROOT"], "/tmp/dfa_123/run/epochs/0001")
        self.assertEqual(captured["env"]["DFA_WORKER_ID"], "worker-a")

    def test_run_agent_retries_after_timeout(self):
        attempts = {"count": 0}

        async def fake_run_with_pi_retry(**kwargs):
            attempts["count"] += 1
            await asyncio.sleep(0.02)
            result = runner.AgentResult()
            result.output = "ok"
            return result

        with patch.object(runner, "_find_pi_command", return_value=["/usr/bin/pi"]):
            with patch.object(runner, "_run_with_pi_retry", side_effect=fake_run_with_pi_retry):
                result = asyncio.run(
                    runner.run_agent(
                        "hello",
                        model="test-model",
                        tools=["read"],
                        cwd=".",
                        run_timeout_seconds=0.01,
                        timeout_retry_enabled=True,
                        timeout_max_retries=1,
                        retry_delay=0,
                    )
                )

        self.assertEqual(attempts["count"], 2)
        self.assertIn("timed out", result.error or "")

    def test_run_agent_triggers_compaction_then_retries_on_context_overflow(self):
        prompts: list[str] = []

        async def fake_run_with_pi_retry(**kwargs):
            prompts.append(kwargs["prompt"])
            if len(prompts) == 1:
                return _overflow_result()
            result = runner.AgentResult()
            result.output = "ok"
            result.exit_code = 0
            return result

        with tempfile.TemporaryDirectory() as cwd:
            with patch.object(runner, "_find_pi_command", return_value=["/usr/bin/pi"]):
                with patch.object(runner, "_run_with_pi_retry", side_effect=fake_run_with_pi_retry):
                    result = asyncio.run(
                        runner.run_agent(
                            "summary",
                            model="MiniMax/MiniMax-M2.5",
                            tools=["read"],
                            cwd=cwd,
                            session_file="/tmp/test-session.jsonl",
                            max_retries=0,
                            pi_max_retries=0,
                        )
                    )

        self.assertEqual(result.output, "ok")
        self.assertEqual(len(prompts), 3)
        self.assertEqual(prompts[0], "summary")
        self.assertIn("compaction", prompts[1].lower())
        self.assertEqual(prompts[2], "summary")


if __name__ == "__main__":
    unittest.main()
