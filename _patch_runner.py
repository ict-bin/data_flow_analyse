import sys, ast, shutil
sys.stdout.reconfigure(encoding='utf-8')

src = open('D:/workspace/pi/system_analyse/app/runner.py', encoding='utf-8').read()

# 1. module & logger
src = src.replace(
    'system_analyse — Agent 子进程执行器',
    'data_flow_analyse — Agent 子进程执行器（RPC 模式）'
).replace(
    'logger = logging.getLogger("sa.runner")',
    'logger = logging.getLogger("dfa.runner")'
)

# 2. system_prompt: write to cwd/.system_prompt.md and use --system-prompt
old_write = (
    '    if system_prompt.strip():\n'
    '        tmp_dir, sys_tmp_file = _write_temp_markdown(\n'
    '            tmp_dir, "sa-", "system.md", system_prompt\n'
    '        )\n'
    '        args.extend(["--append-system-prompt", sys_tmp_file])'
)
new_write = (
    '    if system_prompt.strip():\n'
    '        _sp_path = os.path.join(os.path.abspath(cwd), ".system_prompt.md")\n'
    '        try:\n'
    '            Path(_sp_path).write_text(system_prompt, encoding="utf-8")\n'
    '            sys_tmp_file = _sp_path\n'
    '        except OSError:\n'
    '            tmp_dir, sys_tmp_file = _write_temp_markdown(\n'
    '                tmp_dir, "dfa-", "system.md", system_prompt\n'
    '            )\n'
    '        args.extend(["--system-prompt", sys_tmp_file])'
)
assert old_write in src, 'system_prompt write not found'
src = src.replace(old_write, new_write, 1)
print('system_prompt OK')

# 3. cleanup: keep .system_prompt.md (lives in workspace, cleaned with it)
old_cleanup = (
    '        if sys_tmp_file and os.path.exists(sys_tmp_file):\n'
    '            try:\n'
    '                os.unlink(sys_tmp_file)\n'
    '            except OSError:\n'
    '                pass\n'
    '        if prompt_tmp_file and os.path.exists(prompt_tmp_file):\n'
    '            try:\n'
    '                os.unlink(prompt_tmp_file)\n'
    '            except OSError:\n'
    '                pass\n'
    '        if tmp_dir and os.path.exists(tmp_dir):\n'
    '            try:\n'
    '                os.rmdir(tmp_dir)\n'
    '            except OSError:\n'
    '                pass'
)
new_cleanup = '        pass  # .system_prompt.md is in workspace cwd, cleaned with it'
assert old_cleanup in src, 'cleanup not found'
src = src.replace(old_cleanup, new_cleanup, 1)
print('cleanup OK')

# 4. add post_skill_prompt param to run_agent
old_sig = '    cancel_event: asyncio.Event | None = None,\n    max_retries: int = 3,'
new_sig = (
    '    cancel_event: asyncio.Event | None = None,\n'
    '    post_skill_prompt: str | None = None,\n'
    '    max_retries: int = 3,'
)
assert old_sig in src, 'run_agent sig not found'
src = src.replace(old_sig, new_sig, 1)
print('run_agent sig OK')

# 5. pass post_skill_prompt through _run_with_pi_retry
old_call = (
    '        return await _run_with_pi_retry(\n'
    '            args=args,\n'
    '            cwd=os.path.abspath(cwd),\n'
    '            prompt=prompt,\n'
    '            cancel_event=cancel_event,\n'
    '            on_stream=on_stream,\n'
    '            max_retries=max_retries,\n'
    '            retry_delay=retry_delay,\n'
    '            pi_max_retries=pi_max_retries,\n'
    '            pi_retry_delay=pi_retry_delay,\n'
    '        )'
)
new_call = (
    '        return await _run_with_pi_retry(\n'
    '            args=args,\n'
    '            cwd=os.path.abspath(cwd),\n'
    '            prompt=prompt,\n'
    '            post_skill_prompt=post_skill_prompt,\n'
    '            cancel_event=cancel_event,\n'
    '            on_stream=on_stream,\n'
    '            max_retries=max_retries,\n'
    '            retry_delay=retry_delay,\n'
    '            pi_max_retries=pi_max_retries,\n'
    '            pi_retry_delay=pi_retry_delay,\n'
    '        )'
)
assert old_call in src, 'pi_retry call not found'
src = src.replace(old_call, new_call, 1)
print('pi_retry call OK')

# 6. _run_with_pi_retry signature
old_pi_sig = (
    'async def _run_with_pi_retry(\n'
    '    *,\n'
    '    args: list[str],\n'
    '    cwd: str,\n'
    '    prompt: str,\n'
    '    cancel_event: asyncio.Event | None,'
)
new_pi_sig = (
    'async def _run_with_pi_retry(\n'
    '    *,\n'
    '    args: list[str],\n'
    '    cwd: str,\n'
    '    prompt: str,\n'
    '    post_skill_prompt: str | None = None,\n'
    '    cancel_event: asyncio.Event | None,'
)
assert old_pi_sig in src, '_run_with_pi_retry sig not found'
src = src.replace(old_pi_sig, new_pi_sig, 1)
print('pi_retry sig OK')

# 7. pass to _run_with_api_retry
old_api_call = (
    '            result = await _run_with_api_retry(\n'
    '                args=args,\n'
    '                cwd=cwd,\n'
    '                prompt=prompt,\n'
    '                cancel_event=cancel_event,\n'
    '                on_stream=on_stream,\n'
    '                max_retries=max_retries,\n'
    '                retry_delay=retry_delay,\n'
    '            )'
)
new_api_call = (
    '            result = await _run_with_api_retry(\n'
    '                args=args,\n'
    '                cwd=cwd,\n'
    '                prompt=prompt,\n'
    '                post_skill_prompt=post_skill_prompt,\n'
    '                cancel_event=cancel_event,\n'
    '                on_stream=on_stream,\n'
    '                max_retries=max_retries,\n'
    '                retry_delay=retry_delay,\n'
    '            )'
)
assert old_api_call in src, 'api_retry call not found'
src = src.replace(old_api_call, new_api_call, 1)
print('api_retry call OK')

# 8. _run_with_api_retry signature
old_api_sig = (
    'async def _run_with_api_retry(\n'
    '    *,\n'
    '    args: list[str],\n'
    '    cwd: str,\n'
    '    prompt: str,\n'
    '    cancel_event: asyncio.Event | None,'
)
new_api_sig = (
    'async def _run_with_api_retry(\n'
    '    *,\n'
    '    args: list[str],\n'
    '    cwd: str,\n'
    '    prompt: str,\n'
    '    post_skill_prompt: str | None = None,\n'
    '    cancel_event: asyncio.Event | None,'
)
assert old_api_sig in src, 'api_retry sig not found'
src = src.replace(old_api_sig, new_api_sig, 1)
print('api_retry sig OK')

# 9. Inject post_skill second turn after first agent_end
old_drain = (
    '            if agent_ended:\n'
    '                try:\n'
    '                    async def _drain_stdout():\n'
    '                        assert proc.stdout is not None\n'
    '                        while True:\n'
    '                            chunk = await proc.stdout.read(65536)\n'
    '                            if not chunk:\n'
    '                                break\n'
    '                    await asyncio.wait_for(_drain_stdout(), timeout=10.0)\n'
    '                except (asyncio.TimeoutError, Exception):\n'
    '                    pass'
)

post_skill_block = (
    '            # RPC 第二轮：分析完成后强制调用 skill 输出结果\n'
    '            if agent_ended and post_skill_prompt and proc.stdin and not proc.stdin.is_closing():\n'
    '                try:\n'
    '                    _skill_cmd = json.dumps(\n'
    '                        {"type": "prompt", "message": post_skill_prompt},\n'
    '                        ensure_ascii=False,\n'
    '                    ) + chr(10)\n'
    '                    proc.stdin.write(_skill_cmd.encode("utf-8"))\n'
    '                    await proc.stdin.drain()\n'
    '                    _buf2 = b""\n'
    '                    while True:\n'
    '                        try:\n'
    '                            _chunk2 = await asyncio.wait_for(\n'
    '                                proc.stdout.read(4096), timeout=180.0)\n'
    '                        except asyncio.TimeoutError:\n'
    '                            break\n'
    '                        if not _chunk2:\n'
    '                            break\n'
    '                        _buf2 += _chunk2\n'
    '                        while b"\\n" in _buf2:\n'
    '                            _l2, _buf2 = _buf2.split(b"\\n", 1)\n'
    '                            if _process_line(_l2.decode("utf-8", errors="replace"),\n'
    '                                             result, on_stream):\n'
    '                                break\n'
    '                        else:\n'
    '                            continue\n'
    '                        break\n'
    '                except Exception as _se:\n'
    '                    _log_warn(f"post_skill RPC second turn error (ignored): {_se}")\n'
)

new_drain = post_skill_block + old_drain

assert old_drain in src, 'drain block not found'
src = src.replace(old_drain, new_drain, 1)
print('post_skill injection OK')

ast.parse(src)
print('syntax OK')
open('D:/workspace/pi/data_flow_analyse/app/runner.py', 'w', encoding='utf-8').write(src)
print('written to runner.py')
