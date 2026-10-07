from dp.ingest import source_digest


def _log(tmp_path):
    d = tmp_path / "L1"
    (d / "sensors" / "lidar").mkdir(parents=True)
    (d / "sensors" / "lidar" / "100.feather").write_bytes(b"aaaa")
    (d / "annotations.feather").write_bytes(b"bbbb")
    return d


def test_same_size_content_change_changes_the_source_digest(tmp_path):
    d = _log(tmp_path)
    before = source_digest(d)
    (d / "annotations.feather").write_bytes(b"cccc")
    assert source_digest(d) != before


def test_source_digest_is_stable_and_ignores_the_parent_path(tmp_path):
    a, b = _log(tmp_path / "a"), _log(tmp_path / "b")
    assert source_digest(a) == source_digest(b) == source_digest(a)


def test_a_moved_file_changes_the_source_digest(tmp_path):
    d = _log(tmp_path)
    before = source_digest(d)
    (d / "annotations.feather").rename(d / "annotations2.feather")
    assert source_digest(d) != before
