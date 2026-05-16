import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    from app.models import TaskConfig, TokenUsage
    from app.taint_workflow import PerTaintWorkflow
    _IMPORT_ERROR = None
except ModuleNotFoundError as exc:
    TaskConfig = None  # type: ignore[assignment]
    TokenUsage = None  # type: ignore[assignment]
    PerTaintWorkflow = None  # type: ignore[assignment]
    _IMPORT_ERROR = exc


@unittest.skipIf(_IMPORT_ERROR is not None, f"missing dependency: {_IMPORT_ERROR}")
class UpstreamEntryHintsTests(unittest.TestCase):
    def test_make_result_exposes_upstream_hint_summary_and_markdown_header(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = TaskConfig(
                task="分析 demo.c 中 handle_request 的数据流",
                cwd=tmpdir,
                source_file="demo.c",
                function_name="handle_request",
                taint_params=["buf", "len"],
                function_description="处理外部请求并解析缓冲区。",
                function_description_source="agent",
                entry_reason="由外部请求处理框架调用。",
                entry_reason_source="agent",
                taint_details=[
                    {
                        "name": "buf",
                        "description": "外部输入缓冲区。",
                        "description_source": "agent",
                        "source_kind": "network",
                    }
                ],
                workers={"agents": [{"model": "worker-model"}]},
                judges={"agents": [{"model": "judge-model"}]},
            )
            workflow = PerTaintWorkflow(
                cfg=cfg,
                func_name="handle_request",
                src_file="demo.c",
                taint_params=["buf", "len"],
                task_id="dfa_test_task",
                out_dir=Path(tmpdir),
            )

            result = workflow._make_result(
                "# 数据流追踪: handle_request\n\n正文",
                summary_result=None,
                passed=True,
                rounds=[],
                total_tokens=TokenUsage(),
                completion_reason="passed",
            )

        self.assertEqual(result.upstream_entry_metadata["function_description_source"], "agent")
        self.assertEqual(len(result.taint_hint_summary), 2)
        buf_hint = next(item for item in result.taint_hint_summary if item["name"] == "buf")
        len_hint = next(item for item in result.taint_hint_summary if item["name"] == "len")
        self.assertTrue(buf_hint["has_upstream_hint"])
        self.assertEqual(buf_hint["description_source"], "agent")
        self.assertEqual(buf_hint["source_kind"], "network")
        self.assertFalse(len_hint["has_upstream_hint"])
        self.assertEqual(len_hint["description_source"], "missing")
        self.assertIn("## Upstream Entry Hints", result.final_output)
        self.assertIn("Function Summary [source=agent]", result.final_output)
        self.assertIn("`buf`", result.final_output)
        self.assertIn("worker_prompt, taint_prompt", result.final_output)


if __name__ == "__main__":
    unittest.main()
