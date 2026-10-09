from dp.sample import nearest_frames


def test_picks_the_closest_frame_per_camera():
    assert nearest_frames(100, {"a": [40, 90, 140], "b": [99, 160]}) == {"a": 90, "b": 99}


def test_a_tie_picks_the_earlier_frame():
    assert nearest_frames(100, {"a": [75, 125]}) == {"a": 75}


def test_a_camera_without_frames_is_omitted_not_invented():
    assert nearest_frames(100, {"a": [], "b": [100]}) == {"b": 100}


def test_before_the_first_and_after_the_last_frame():
    assert nearest_frames(10, {"a": [50, 100]}) == {"a": 50}
    assert nearest_frames(500, {"a": [50, 100]}) == {"a": 100}
