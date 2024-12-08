import subprocess


def test_extract():
    """Tests the extract() function"""
    result = subprocess.run(
        ["python", "main.py", "extract"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0
    assert "Extracting data..." in result.stdout


def test_transform_load():
    """Tests the transform_and_load() function"""
    result = subprocess.run(
        ["python", "main.py", "transform_load"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0
    assert "Transforming and loading data..." in result.stdout


def test_general_query():
    """Tests the execute_query() function"""
    result = subprocess.run(
        ["python", "main.py", "general_query"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0


if __name__ == "__main__":
    test_extract()
    test_transform_load()
    test_general_query()
