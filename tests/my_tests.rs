use std::process::Command;

#[test]
fn test_extract() {
    let result = Command::new("cargo")
        .arg("run")
        .arg("--bin")
        .arg("int6_individual2")
        .arg("extract")
        .output();

    if let Err(e) = result {
        eprintln!("Failed to execute extract command: {:?}", e);
        assert!(false, "Test `test_extract` failed: {:?}", e);
    } else if !result.unwrap().status.success() {
        eprintln!("Extract command did not complete successfully.");
        assert!(false, "Test `test_extract` failed.");
    }
}

#[test]
fn test_transform_load() {
    let result = Command::new("cargo")
        .arg("run")
        .arg("--bin")
        .arg("int6_individual2")
        .arg("transform_load")
        .output();

    if let Err(e) = result {
        eprintln!("Failed to execute transform_load command: {:?}", e);
        assert!(false, "Test `test_transform_load` failed: {:?}", e);
    } else if !result.unwrap().status.success() {
        eprintln!("Transform and load command did not complete successfully.");
        assert!(false, "Test `test_transform_load` failed.");
    }
}

#[test]
fn test_crud_operations() {
    let result = Command::new("cargo")
        .arg("run")
        .arg("--bin")
        .arg("int6_individual2")
        .arg("crud")
        .output();

    if let Err(e) = result {
        eprintln!("Failed to execute CRUD operations: {:?}", e);
        assert!(false, "Test `test_crud_operations` failed: {:?}", e);
    } else if !result.unwrap().status.success() {
        eprintln!("CRUD operations command did not complete successfully.");
        assert!(false, "Test `test_crud_operations` failed.");
    }
}

#[test]
fn test_queries() {
    let result = Command::new("cargo")
        .arg("run")
        .arg("--bin")
        .arg("int6_individual2")
        .arg("query")
        .output();

    if let Err(e) = result {
        eprintln!("Failed to execute query command: {:?}", e);
        assert!(false, "Test `test_queries` failed: {:?}", e);
    } else if !result.unwrap().status.success() {
        eprintln!("Query command did not complete successfully.");
        assert!(false, "Test `test_queries` failed.");
    }
}

#[test]
fn test_find_user_by_id() {
    // Run the `find_user` command with the specified user ID
    let result = Command::new("cargo")
        .arg("run")
        .arg("--bin")
        .arg("int6_individual2")
        .arg("find_user")
        .arg("1001")
        .output();

    // Check if the command executed successfully
    if let Err(e) = result {
        eprintln!("Failed to execute find_user command: {:?}", e);
        assert!(false, "Test `test_find_user_by_id` failed: {:?}", e);
    } else {
        let output = result.unwrap();
        if !output.status.success() {
            eprintln!("find_user command did not complete successfully.");
            assert!(false, "Test `test_find_user_by_id` failed.");
        } else {
            // Convert output to string for assertion checks
            let stdout =
                std::str::from_utf8(&output.stdout).expect("Failed to parse output as UTF-8");
            // Check if the output contains expected data
            assert!(stdout.contains("1001"), "Expected ID not found in output");
            assert!(stdout.contains("| Yes    | Yes      | Yes           | 4    | 4     | 5     | 1    | 1    | 9      | 1     |"), "Expected 'Yes' value for cancer not found in output");
        }
    }
}
