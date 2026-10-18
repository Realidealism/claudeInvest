"""The rebuild guard: tools/build_info.py.

On 2026-10-15 rebuilds from the main tree replaced, unnoticed, a deployment
built from two branches the tree did not have. `check` now refuses a build
whose HEAD does not contain the commit recorded for the deployed exes, and
`record` writes that commit after each build. git is a stand-in here.
"""
import json
import subprocess
import types

import pytest

import tools.build_info as bi

HEAD = "c0ffee" * 6 + "abcd"
DEPLOYED = "deadbeef" * 5


class _Git:
    """Answers the git calls build_info makes."""

    def __init__(self, ancestor=True, dirty=""):
        self.ancestor, self.dirty = ancestor, dirty
        self.calls = []

    def __call__(self, *args):
        self.calls.append(args)
        out, rc = "", 0
        if args[:2] == ("rev-parse", "HEAD"):
            out = HEAD + "\n"
        elif args[:2] == ("rev-parse", "--abbrev-ref"):
            out = "main-line\n"
        elif args[0] == "status":
            out = self.dirty
        elif args[:2] == ("merge-base", "--is-ancestor"):
            rc = 0 if self.ancestor else 1
        return subprocess.CompletedProcess(args, rc, stdout=out, stderr="")


@pytest.fixture
def info(tmp_path, monkeypatch):
    path = tmp_path / "BUILD_INFO.json"
    monkeypatch.setattr(bi, "INFO", path)
    return path


def deployed(info, commit=DEPLOYED):
    info.write_text(json.dumps({"commit": commit}), encoding="utf-8")


def test_first_build_has_nothing_to_compare(info, monkeypatch, capsys):
    monkeypatch.setattr(bi, "_git", _Git())
    assert bi.main(["check"]) == 0
    assert "nothing to compare" in capsys.readouterr().out


def test_build_containing_the_deployed_commit_goes_ahead(info, monkeypatch):
    git = _Git(ancestor=True)
    monkeypatch.setattr(bi, "_git", git)
    deployed(info)
    assert bi.main(["check"]) == 0
    assert ("merge-base", "--is-ancestor", DEPLOYED, HEAD) in git.calls


def test_build_missing_the_deployed_commit_is_refused(info, monkeypatch, capsys):
    monkeypatch.setattr(bi, "_git", _Git(ancestor=False))
    deployed(info)
    assert bi.main(["check"]) == bi.REFUSED == 5
    assert "REFUSED" in capsys.readouterr().out


def test_allow_older_overrides_the_refusal(info, monkeypatch, capsys):
    monkeypatch.setattr(bi, "_git", _Git(ancestor=False))
    deployed(info)
    # The .bat passes all its arguments through.
    assert bi.main(["check", "--no-restart", "--allow-older"]) == 0
    assert "WARNING" in capsys.readouterr().out


def test_uncommitted_sources_warn_but_do_not_block(info, monkeypatch, capsys):
    git = _Git(ancestor=True, dirty=" M db/industry_data.py\n?? tools/new.py\n")
    monkeypatch.setattr(bi, "_git", git)
    deployed(info)
    assert bi.main(["check"]) == 0
    out = capsys.readouterr().out
    assert "db/industry_data.py" in out and "tools/new.py" in out
    assert ("status", "--porcelain", "--", "*.py", "*.spec") in git.calls


def test_record_writes_what_was_built(info, monkeypatch):
    monkeypatch.setattr(bi, "_git", _Git(dirty=" M db/industry_data.py\n"))
    assert bi.main(["record"]) == 0
    written = json.loads(info.read_text(encoding="utf-8"))
    assert written["commit"] == HEAD and written["branch"] == "main-line"
    assert written["uncommitted_sources"] == ["db/industry_data.py"]
    assert written["built_at"]


def test_output_is_ascii(info, monkeypatch, capsys):
    # The .bat appends it to an ASCII log.
    monkeypatch.setattr(bi, "_git", _Git(ancestor=False))
    deployed(info)
    bi.main(["check"])
    bi.main(["record"])
    capsys.readouterr().out.encode("ascii")


def test_unknown_command_is_an_error(capsys):
    assert bi.main(["deploy"]) == 2
