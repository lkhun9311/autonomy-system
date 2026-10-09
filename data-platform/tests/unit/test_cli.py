import pytest

from dp.cli import main


def test_fetch_runs_from_any_working_directory(tmp_path, monkeypatch):
    ids = tmp_path / "ids.txt"
    ids.write_text("")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DP_DATA", str(tmp_path / "data"))
    assert main(["fetch", str(ids)]) == 0
    assert (tmp_path / "data" / "sensor" / "val").is_dir()


@pytest.mark.parametrize("bad", ["x' or '1'='1", "a b", "../etc", ""])
def test_log_ids_that_could_change_a_query_are_refused(bad, capsys):
    with pytest.raises(SystemExit) as e:
        main(["reconcile", bad])
    assert e.value.code == 2
    assert "log id" in capsys.readouterr().err
