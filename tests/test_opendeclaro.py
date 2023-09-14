"""main test python script"""
from opendeclaro import __version__


def test_version() -> None:
    """Test function to check version"""
    assert __version__ == "0.1.0"
