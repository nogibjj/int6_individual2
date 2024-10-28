use csv::{ReaderBuilder, Writer};
use prettytable::{row, Cell, Row, Table};
use reqwest::blocking::Client;
use rusqlite::{params, Connection, Result};
use std::error::Error;
use std::fs;
use std::fs::File;

pub fn extract(url: &str, file_path: &str, directory: &str) -> Result<String, Box<dyn Error>> {
    // Create directory if it doesn't exist
    if fs::metadata(directory).is_err() {
        fs::create_dir_all(directory)?;
    }

    // Initialize HTTP client and send request
    let client = Client::new();
    let mut response = client.get(url).send()?;

    // Check if request was successful
    if response.status().is_success() {
        // Write response content to file
        let mut file = File::create(file_path)?;
        std::io::copy(&mut response, &mut file)?;

        println!("Data successfully downloaded to: {}", file_path);

        // Open the CSV to take a subset of data
        let mut reader = ReaderBuilder::new().from_path(file_path)?;

        // Define the columns to include in the subset
        let headers_to_keep = vec![
            "ID",
            "cancer",
            "diabetes",
            "heart_disease",
            "EGGSFREQ",
            "GREENSALADFREQ",
            "FRIESFREQ",
            "MILKFREQ",
            "SODAFREQ",
            "COFFEEFREQ",
            "CAKESFREQ",
        ];

        // Filter the headers to get their indices in the CSV
        let header_indices: Vec<usize> = headers_to_keep
            .iter()
            .filter_map(|&header| reader.headers().ok()?.iter().position(|h| h == header))
            .collect();

        // Define the output path for the subset
        let subset_file_path = format!("{}/Nutrition_subset.csv", directory);
        let mut writer = Writer::from_path(&subset_file_path)?;

        // Write headers for the subset file
        writer.write_record(&headers_to_keep)?;

        // Iterate over records, taking only the first 100 rows
        for (i, result) in reader.records().enumerate() {
            if i >= 100 {
                break;
            }
            let record = result?;
            let subset_row: Vec<&str> = header_indices
                .iter()
                .map(|&index| record.get(index).unwrap_or(""))
                .collect();
            writer.write_record(&subset_row)?;
        }

        println!("Subset successfully created at: {}", subset_file_path);
        Ok(subset_file_path) // Return the subset path on success
    } else {
        Err(Box::from(format!(
            "Failed to download data. Status code: {}",
            response.status()
        )))
    }
}

pub fn transform_load(dataset: &str) -> Result<String, Box<dyn Error>> {
    // Print current working directory
    println!("Current working directory: {:?}", std::env::current_dir()?);

    // Open the subset CSV
    let mut reader = ReaderBuilder::new().from_path(dataset)?;

    // Connect to SQLite and create the Nutrition database
    let conn = Connection::open("Nutrition.db")?;

    // Drop the table if it already exists
    conn.execute("DROP TABLE IF EXISTS Nutrition", [])?;

    // Create the table with specified columns
    conn.execute(
        "CREATE TABLE Nutrition (
            ID INTEGER PRIMARY KEY,
            cancer TEXT,
            diabetes TEXT,
            heart_disease TEXT,
            EGGSFREQ INTEGER,
            GREENSALADFREQ INTEGER,
            FRIESFREQ INTEGER,
            MILKFREQ INTEGER,
            SODAFREQ INTEGER,
            COFFEEFREQ INTEGER,
            CAKESFREQ INTEGER
        )",
        [],
    )?;

    // Prepare to insert each row from the CSV file
    let mut insert_stmt = conn.prepare(
        "INSERT INTO Nutrition (
            ID, cancer, diabetes, heart_disease, 
            EGGSFREQ, GREENSALADFREQ, FRIESFREQ, 
            MILKFREQ, SODAFREQ, COFFEEFREQ, CAKESFREQ
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )?;

    // Read CSV rows and insert into the database
    for result in reader.records() {
        let record = result?;
        // Convert CSV row to a vector of values matching table columns
        insert_stmt.execute(params![
            record.get(0).unwrap_or("").parse::<i32>()?,  // ID
            record.get(1).unwrap_or(""),                  // cancer
            record.get(2).unwrap_or(""),                  // diabetes
            record.get(3).unwrap_or(""),                  // heart_disease
            record.get(4).unwrap_or("").parse::<i32>()?,  // EGGSFREQ
            record.get(5).unwrap_or("").parse::<i32>()?,  // GREENSALADFREQ
            record.get(6).unwrap_or("").parse::<i32>()?,  // FRIESFREQ
            record.get(7).unwrap_or("").parse::<i32>()?,  // MILKFREQ
            record.get(8).unwrap_or("").parse::<i32>()?,  // SODAFREQ
            record.get(9).unwrap_or("").parse::<i32>()?,  // COFFEEFREQ
            record.get(10).unwrap_or("").parse::<i32>()?, // CAKESFREQ
        ])?;
    }

    println!("Data successfully loaded into Nutrition.db");

    Ok("Nutrition.db".to_string()) // Return the database path
}

// Create the Nutrition table if it doesn't exist
pub fn create_table() -> Result<()> {
    let conn = Connection::open("Nutrition.db")?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS Nutrition (
            ID INTEGER PRIMARY KEY,
            cancer TEXT,
            diabetes TEXT,
            heart_disease TEXT,
            EGGSFREQ INTEGER,
            GREENSALADFREQ INTEGER,
            FRIESFREQ INTEGER,
            MILKFREQ INTEGER,
            SODAFREQ INTEGER,
            COFFEEFREQ INTEGER,
            CAKESFREQ INTEGER
        )",
        [],
    )?;
    println!("Table created successfully.");
    Ok(())
}

// Insert sample data into the Nutrition table
pub fn insert_data() -> Result<()> {
    println!("Inserting Data");
    let conn = Connection::open("Nutrition.db")?;
    conn.execute(
        "INSERT INTO Nutrition (ID, cancer, diabetes, heart_disease, EGGSFREQ, GREENSALADFREQ, FRIESFREQ, MILKFREQ, SODAFREQ, COFFEEFREQ, CAKESFREQ)
         VALUES (1, 'Yes', 'No', 'No', 2, 5, 3, 4, 1, 2, 3)",
        [],
    )?;
    conn.execute(
        "INSERT INTO Nutrition (ID, cancer, diabetes, heart_disease, EGGSFREQ, GREENSALADFREQ, FRIESFREQ, MILKFREQ, SODAFREQ, COFFEEFREQ, CAKESFREQ)
         VALUES (2, 'No', 'Yes', 'Yes', 4, 3, 2, 5, 2, 4, 1)",
        [],
    )?;
    println!("Sample data inserted successfully.");
    Ok(())
}

// Read and display the top 5 rows of the Nutrition table
pub fn read_data() -> Result<()> {
    let conn = Connection::open("Nutrition.db")?;
    let mut stmt = conn.prepare("SELECT * FROM Nutrition LIMIT 5")?;
    let nutrition_iter = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i32>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i32>(4)?,
            row.get::<_, i32>(5)?,
            row.get::<_, i32>(6)?,
            row.get::<_, i32>(7)?,
            row.get::<_, i32>(8)?,
            row.get::<_, i32>(9)?,
            row.get::<_, i32>(10)?,
        ))
    })?;

    let mut table = Table::new();
    table.add_row(row![
        "ID",
        "Cancer",
        "Diabetes",
        "Heart Disease",
        "Eggs",
        "Salad",
        "Fries",
        "Milk",
        "Soda",
        "Coffee",
        "Cakes"
    ]);

    for record in nutrition_iter {
        let (id, cancer, diabetes, heart_disease, eggs, salad, fries, milk, soda, coffee, cakes) =
            record?;
        table.add_row(Row::new(vec![
            Cell::new(&id.to_string()),
            Cell::new(&cancer),
            Cell::new(&diabetes),
            Cell::new(&heart_disease),
            Cell::new(&eggs.to_string()),
            Cell::new(&salad.to_string()),
            Cell::new(&fries.to_string()),
            Cell::new(&milk.to_string()),
            Cell::new(&soda.to_string()),
            Cell::new(&coffee.to_string()),
            Cell::new(&cakes.to_string()),
        ]));
    }
    table.printstd();
    Ok(())
}

// Update a specific record in the Nutrition table
pub fn update_data() -> Result<()> {
    println!("Updating ID 1 to 6 Eggs");
    let conn = Connection::open("Nutrition.db")?;
    conn.execute(
        "UPDATE Nutrition SET EGGSFREQ = 6, cancer = 'No' WHERE ID = 1",
        [],
    )?;
    println!("Record updated successfully.");
    Ok(())
}

// Delete a specific record from the Nutrition table
pub fn delete_data() -> Result<()> {
    println!("Deleting Data");
    let conn = Connection::open("Nutrition.db")?;
    conn.execute("DELETE FROM Nutrition WHERE ID = 2", [])?;
    println!("Record deleted successfully.");
    Ok(())
}

// Retrieve and display records with high soda frequency
pub fn query_frequent_soda() -> Result<()> {
    let conn = Connection::open("Nutrition.db")?;
    let mut stmt = conn.prepare(
        "SELECT ID, SODAFREQ, EGGSFREQ, FRIESFREQ FROM Nutrition WHERE SODAFREQ > 3 LIMIT 5",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i32>(0)?,
            row.get::<_, i32>(1)?,
            row.get::<_, i32>(2)?,
            row.get::<_, i32>(3)?,
        ))
    })?;

    let mut table = Table::new();
    table.add_row(row![
        "ID",
        "Soda Frequency",
        "Eggs Frequency",
        "Fries Frequency"
    ]);

    for row in rows {
        let (id, soda_freq, eggs_freq, fries_freq) = row?;
        table.add_row(Row::new(vec![
            Cell::new(&id.to_string()),
            Cell::new(&soda_freq.to_string()),
            Cell::new(&eggs_freq.to_string()),
            Cell::new(&fries_freq.to_string()),
        ]));
    }
    table.printstd();
    Ok(())
}

// Retrieve and display records with heart disease
pub fn query_heart_disease() -> Result<()> {
    let conn = Connection::open("Nutrition.db")?;
    let mut stmt = conn.prepare("SELECT ID, EGGSFREQ, GREENSALADFREQ, FRIESFREQ, SODAFREQ FROM Nutrition WHERE heart_disease = 'Yes' LIMIT 5")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i32>(0)?,
            row.get::<_, i32>(1)?,
            row.get::<_, i32>(2)?,
            row.get::<_, i32>(3)?,
            row.get::<_, i32>(4)?,
        ))
    })?;

    let mut table = Table::new();
    table.add_row(row![
        "ID",
        "Eggs Frequency",
        "Salad Frequency",
        "Fries Frequency",
        "Soda Frequency"
    ]);

    for row in rows {
        let (id, eggs_freq, salad_freq, fries_freq, soda_freq) = row?;
        table.add_row(Row::new(vec![
            Cell::new(&id.to_string()),
            Cell::new(&eggs_freq.to_string()),
            Cell::new(&salad_freq.to_string()),
            Cell::new(&fries_freq.to_string()),
            Cell::new(&soda_freq.to_string()),
        ]));
    }
    table.printstd();
    Ok(())
}

// Function to run all CRUD operations
pub fn run_crud_operations() -> Result<()> {
    create_table()?;
    read_data()?;
    insert_data()?;
    read_data()?;
    update_data()?;
    read_data()?;
    delete_data()?;
    read_data()?;
    Ok(())
}

/// Function to query and display user information by user ID
pub fn find_user_by_id(user_id: i32) -> Result<()> {
    // Connect to the Nutrition database
    let conn = Connection::open("Nutrition.db")?;

    // Prepare and execute the SQL query
    let mut stmt = conn.prepare("SELECT * FROM Nutrition WHERE ID = ?1")?;
    let mut rows = stmt.query(params![user_id])?;

    // Set up a table for pretty output
    let mut table = Table::new();
    table.add_row(row![
        "ID",
        "Cancer",
        "Diabetes",
        "Heart Disease",
        "Eggs",
        "Salad",
        "Fries",
        "Milk",
        "Soda",
        "Coffee",
        "Cakes"
    ]);

    // Fetch and display the result if found
    if let Some(row) = rows.next()? {
        table.add_row(row![
            row.get::<_, i32>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i32>(4)?,
            row.get::<_, i32>(5)?,
            row.get::<_, i32>(6)?,
            row.get::<_, i32>(7)?,
            row.get::<_, i32>(8)?,
            row.get::<_, i32>(9)?,
            row.get::<_, i32>(10)?
        ]);
        table.printstd();
    } else {
        println!("No user found with ID: {}", user_id);
    }

    Ok(())
}
