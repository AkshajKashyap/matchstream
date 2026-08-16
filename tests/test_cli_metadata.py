from __future__ import annotations

from matchstream.cli import main


def test_project_info_is_machine_readable(monkeypatch, capsys) -> None:
    monkeypatch.setattr("sys.argv", ["matchstream", "project-info"])

    main()

    assert '"version": "1.0.0"' in capsys.readouterr().out
