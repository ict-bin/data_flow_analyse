import json
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.parsers import _read_tainted_list
from app.taint_workflow import PerTaintWorkflow


class TaintvarsTests(unittest.TestCase):
    def test_read_tainted_list_ignores_taintvar_records(self):
        with tempfile.TemporaryDirectory() as td:
            task_dir = Path(td)
            ws = task_dir / "workspace-worker-0"
            ws.mkdir(parents=True, exist_ok=True)
            (ws / "tainted.list").write_text(
                "@taintvar###out_var###L123###RecvPacket(output-param)\n"
                "src/foo.c###Foo###L200###out_var\n",
                encoding="utf-8",
            )
            callees = _read_tainted_list(str(task_dir))
            self.assertEqual(len(callees), 1)
            self.assertEqual(callees[0].function_name, "Foo")
            self.assertEqual(callees[0].tainted_params, "out_var")

    def test_gen_tainted_list_writes_taintvars_json(self):
        with tempfile.TemporaryDirectory() as td:
            script = Path(__file__).resolve().parent / "tools" / "gen_tainted_list.py"
            input_text = (
                "@taintvar###out_var###L123###RecvPacket(output-param)\n"
                "src/foo.c###Foo###L200###out_var\n"
            )
            proc = subprocess.run(
                [sys.executable, str(script)],
                input=input_text,
                text=True,
                cwd=td,
                capture_output=True,
                check=True,
            )
            self.assertIn("taintvars.json", proc.stdout)
            taintvars = json.loads(Path(td, "taintvars.json").read_text(encoding="utf-8"))
            self.assertEqual(len(taintvars), 1)
            self.assertEqual(taintvars[0]["name"], "out_var")
            tainted_list = Path(td, "tainted.list").read_text(encoding="utf-8")
            self.assertIn("src/foo.c###Foo###L200###out_var", tainted_list)

    def test_append_taintvar_callees_from_source_regression_ipsec_outparam(self):
        with tempfile.TemporaryDirectory() as td:
            ws = Path(td)
            (ws / "taintvars.json").write_text(
                json.dumps([
                    {"name": "mbuf", "line": "L26579", "source": "SOCK_RecvMbufEx_fl", "kind": "output-param"}
                ], ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            workflow = PerTaintWorkflow.__new__(PerTaintWorkflow)
            workflow.ws = ws
            workflow.src_file = "core/ipsec/libipsec.c"
            workflow.func_name = "IPSEC_SOCK_ProcPipeData"
            workflow.line_hint = "L26540"
            workflow.cfg = types.SimpleNamespace(cwd=td)

            func_body = "\n".join([
                "// core/ipsec/libipsec.c  L26540-L26590  (51 lines)",
                "L26579: status = (int)SOCK_RecvMbufEx_fl(recv_pipe_id, recv_len, &mbuf, 0, &recv_status, ctx_base + CTX_RECV_CFG_OFF, \"IPSEC_SOCK_ProcPipeData\", 148);",
                "L26612: status = (int)IPSEC_LIBI_HandleOutputPkt(lib_ctx, mbuf, &sa_type);",
                "L26660: status = (int)IPSEC_SOCK_Buffer_Packet(cong_node, mbuf, ctx_base);",
                "L26684: send_ret = (unsigned int)IPSEC_SOCK_SendToSocket(recv_pipe_id, sock_state, mbuf, ctx_base, vr_entry);",
                "L26780: send_ret = (unsigned int)IPSEC_SOCK_SendToPP6orPP4orLDMPipe(mbuf, ctx_base, sock_state, vr_entry, ldm_pipe);",
            ])

            out_lines = []
            seen = set()
            with mock.patch("app.taint_workflow._extract_function_body", return_value=func_body), \
                 mock.patch("app.cpp_resolver._function_has_definition", side_effect=lambda cwd, fn: fn in {
                     "IPSEC_LIBI_HandleOutputPkt", "IPSEC_SOCK_Buffer_Packet", "IPSEC_SOCK_SendToSocket", "IPSEC_SOCK_SendToPP6orPP4orLDMPipe"
                 }), \
                 mock.patch("app.cpp_resolver._resolve_cpp_name", side_effect=lambda cwd, fn, sf: (fn, "core/ipsec/libipsec.c")), \
                 mock.patch("app.cpp_resolver._get_definition_line", side_effect=lambda cwd, fn, rf: "L999"):
                workflow._append_taintvar_callees_from_source(out_lines, seen)

            self.assertIn("core/ipsec/libipsec.c###IPSEC_LIBI_HandleOutputPkt###L999###mbuf", out_lines)
            self.assertIn("core/ipsec/libipsec.c###IPSEC_SOCK_Buffer_Packet###L999###mbuf", out_lines)
            self.assertIn("core/ipsec/libipsec.c###IPSEC_SOCK_SendToSocket###L999###mbuf", out_lines)
            self.assertIn("core/ipsec/libipsec.c###IPSEC_SOCK_SendToPP6orPP4orLDMPipe###L999###mbuf", out_lines)


if __name__ == "__main__":
    unittest.main()
