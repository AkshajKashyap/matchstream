from matchstream import __version__


def test_release_version_is_exposed_by_the_package() -> None:
    assert __version__ == "1.0.0"
