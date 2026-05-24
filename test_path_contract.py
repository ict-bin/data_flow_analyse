import sys
import tempfile
import unittest
import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.db.models import Base
from app.service.task_service import TaskService, _normalize_source_file_for_root


class DfaPathContractTests(unittest.TestCase):
    def test_normalize_source_file_for_root_returns_relative_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "src"
            target = root / "pkg" / "demo.c"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("int demo(void) { return 0; }\n", encoding="utf-8")

            normalized = _normalize_source_file_for_root(str(root), str(target))

        self.assertEqual("pkg/demo.c", normalized)

    def test_create_task_persists_module_and_source_paths_separately(self):
        with tempfile.TemporaryDirectory() as tmp:
            files_root = Path(tmp) / "files"
            module_root = files_root / "project" / "module"
            source_root = files_root / "project" / "source"
            source_file = source_root / "pkg" / "demo.c"
            module_root.mkdir(parents=True, exist_ok=True)
            source_file.parent.mkdir(parents=True, exist_ok=True)
            source_file.write_text("int demo(void) { return 0; }\n", encoding="utf-8")

            engine = create_engine("sqlite:///:memory:")
            Base.metadata.create_all(bind=engine)
            SessionLocal = sessionmaker(bind=engine)
            db = SessionLocal()
            service = TaskService()
            previous_fileserver_root = os.environ.get("FILESERVER_ROOT")
            os.environ["FILESERVER_ROOT"] = str(files_root)
            try:
                original_input = service.create_task(
                    db,
                    project_id="p1",
                    task_name="dfa-demo",
                    input_path=str(module_root),
                    module_input_path=str(module_root),
                    source_root_path=str(source_root),
                    output_path=str(files_root / "project" / "output"),
                    task_description="demo",
                    prompt_content="分析 pkg/demo.c 中 demo 函数",
                    task_config_json={"source_file": str(source_file), "definition_kind": "definition"},
                )
            finally:
                if previous_fileserver_root is None:
                    os.environ.pop("FILESERVER_ROOT", None)
                else:
                    os.environ["FILESERVER_ROOT"] = previous_fileserver_root

        self.assertEqual(str(module_root.resolve()), original_input["module_input_path"])
        self.assertEqual(str(source_root.resolve()), original_input["source_root_path"])
        self.assertEqual(str(module_root.resolve()), original_input["input_path"])
        self.assertEqual("definition", original_input["definition_kind"])
        self.assertEqual("pkg/demo.c", original_input["task_config_json"]["source_file"])


if __name__ == "__main__":
    unittest.main()
