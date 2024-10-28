import subprocess


def test_extract():
    """Tests the extract function"""
    result = subprocess.run(
        ["python", "main.py", "extract"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0, "Extract command failed"
    assert "Extracting data..." in result.stdout


def test_transform_load():
    """Tests the transform and load function"""
    result = subprocess.run(
        ["python", "main.py", "transform_load"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0, "Transform and load command failed"
    assert "Transforming data..." in result.stdout


def test_crud_operations():
    """Tests the CRUD operations"""
    result = subprocess.run(
        ["python", "main.py", "run_crud_operations"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0, "CRUD operations failed"
    assert "CRUD on data..." in result.stdout


def test_queries():
    """Tests the SQL queries"""
    result = subprocess.run(
        ["python", "main.py", "query_frequent_soda"],
        capture_output=True,
        text=True,
        check=True,
    )
    result2 = subprocess.run(
        ["python", "main.py", "query_heart_disease"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0, "SQL queries failed"
    assert result2.returncode == 0, "SQL queries failed"
    assert "Querying data..." in result.stdout


if __name__ == "__main__":
    test_extract()
    test_transform_load()
    test_crud_operations()
    test_queries()
