"""The rebuild guard: tools/build_info.py.

On 2026-10-15 rebuilds from the main tree replaced, unnoticed, a deployment
built from two branches the tree did not have. `check` now refuses a build
whose HEAD does not contain the commit recorded for the deployed exes (or
when exes exist without a record, or a migration is uncommitted), and
`record` writes that commit after each build. git is a stand-in here.
"""
import json
import subprocess

import pytest

import tools.build_info as bi

HEAD = "c0ffee" * 6 + "abcd"
DEPLOYED = "deadbeef" * 5


class _Git:
    """Answers the git calls build_info makes. ancestor: rc of merge-base
    (0 ancestor, 1 not, 128 git could not resolve it)."""

    def __init__(self, ancestor=0, dirty="", migrations="", head=HEAD):
        self.ancestor, self.dirty, self.migrations, self.head = ancestor, dirty, migrations, head
        self.calls = []

    def __call__(self, *args):
        self.calls.append(args)
        out, err, rc = "", "", 0
        if args[:2] == ("rev-parse", "HEAD"):
            out, rc = (self.head + "\n", 0) if self.head else ("", 128)
            err = "" if self.head else "fatal: not a git repository"
        elif args[:2] == ("rev-parse", "--abbrev-ref"):
            out = "main-line\n"
        elif args[0] == "status":
            out = self.migrations if "db/migrations/*.sql" in args else self.dirty
        elif args[:2] == ("merge-base", "--is-ancestor"):
            rc = self.ancestor
            err = "fatal: Not a valid commit name" if rc == 128 else ""
        return subprocess.CompletedProcess(args, rc, stdout=out, stderr=err)


@pytest.fixture
def dist(tmp_path, monkeypatch):
    monkeypatch.setattr(bi, "INFO", tmp_path / "BUILD_INFO.json")
    return tmp_path


def deployed(dist, commit=DEPLOYED):
    for exe in bi.EXES:
        (dist / exe).write_bytes(b"MZ")
    (dist / "BUILD_INFO.json").write_text(json.dumps({"commit": commit}), encoding="utf-8")


def exes_only(dist):
    for exe in bi.EXES:
        (dist / exe).write_bytes(b"MZ")


def test_first_build_into_an_empty_dist_goes_ahead(dist, monkeypatch, capsys):
    monkeypatch.setattr(bi, "_git", _Git())
    assert bi.main(["check"]) == 0
    assert "nothing to compare" in capsys.readouterr().out


def test_exes_without_a_record_are_refused(dist, monkeypatch, capsys):
    # A deleted record, or a build that stopped half way (the .bat drops the
    # record before building), must not silently turn the guard off.
    monkeypatch.setattr(bi, "_git", _Git())
    exes_only(dist)
    assert bi.main(["check"]) == bi.REFUSED == 5
    assert "no BUILD_INFO.json" in capsys.readouterr().out
    assert bi.main(["check", "--allow-older"]) == 0


def test_build_containing_the_deployed_commit_goes_ahead(dist, monkeypatch):
    git = _Git(ancestor=0)
    monkeypatch.setattr(bi, "_git", git)
    deployed(dist)
    assert bi.main(["check"]) == 0
    assert ("merge-base", "--is-ancestor", DEPLOYED, HEAD) in git.calls


def test_build_missing_the_deployed_commit_is_refused(dist, monkeypatch, capsys):
    monkeypatch.setattr(bi, "_git", _Git(ancestor=1))
    deployed(dist)
    assert bi.main(["check"]) == bi.REFUSED
    assert "not in the history" in capsys.readouterr().out


def test_git_that_cannot_compare_says_so_and_refuses(dist, monkeypatch, capsys):
    monkeypatch.setattr(bi, "_git", _Git(ancestor=128))
    deployed(dist)
    assert bi.main(["check"]) == bi.REFUSED
    out = capsys.readouterr().out
    assert "could not compare" in out and "Not a valid commit name" in out


def test_allow_older_overrides_the_refusal(dist, monkeypatch, capsys):
    monkeypatch.setattr(bi, "_git", _Git(ancestor=1))
    deployed(dist)
    # The .bat passes all its arguments through.
    assert bi.main(["check", "--no-restart", "--allow-older"]) == 0
    assert "WARNING" in capsys.readouterr().out


def test_uncommitted_sources_warn_but_do_not_block(dist, monkeypatch, capsys):
    git = _Git(dirty=" M db/industry_data.py\n?? tools/new.py\n")
    monkeypatch.setattr(bi, "_git", git)
    deployed(dist)
    assert bi.main(["check"]) == 0
    out = capsys.readouterr().out
    assert "db/industry_data.py" in out and "tools/new.py" in out


def test_uncommitted_migration_is_refused(dist, monkeypatch, capsys):
    # The exes bundle db/migrations; init_db applies anything new on start.
    monkeypatch.setattr(bi, "_git", _Git(migrations="?? db/migrations/118_new.sql\n"))
    deployed(dist)
    assert bi.main(["check"]) == bi.REFUSED
    assert "118_new.sql" in capsys.readouterr().out
    assert bi.main(["check", "--allow-older"]) == bi.REFUSED
    assert bi.main(["check", "--allow-migrations"]) == 0


def test_record_writes_what_was_built(dist, monkeypatch):
    monkeypatch.setattr(bi, "_git", _Git(dirty=" M db/industry_data.py\n"))
    assert bi.main(["record"]) == 0
    written = json.loads((dist / "BUILD_INFO.json").read_text(encoding="utf-8"))
    assert written["commit"] == HEAD and written["branch"] == "main-line"
    assert written["exes"] == list(bi.EXES)
    assert written["uncommitted_sources"] == ["db/industry_data.py"]
    assert written["built_at"]


def test_record_without_a_head_writes_nothing_and_fails(dist, monkeypatch):
    monkeypatch.setattr(bi, "_git", _Git(head=""))
    assert bi.main(["record"]) == 1
    assert not (dist / "BUILD_INFO.json").exists()


def test_output_is_ascii(dist, monkeypatch, capsys):
    # The .bat appends it to an ASCII log.
    monkeypatch.setattr(bi, "_git", _Git(ancestor=128, migrations="?? db/migrations/x.sql\n"))
    deployed(dist)
    bi.main(["check"])
    bi.main(["check", "--allow-migrations"])
    bi.main(["record"])
    capsys.readouterr().out.encode("ascii")


def test_unknown_command_is_an_error():
    assert bi.main(["deploy"]) == 2
