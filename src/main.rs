use int6_individual2::{
    extract, find_user_by_id, query_frequent_soda, query_heart_disease, run_crud_operations,
    transform_load,
};
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Please specify a function to run: extract, transform_load, crud, or query.");
        return;
    }

    match args[1].as_str() {
        "extract" => {
            let url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/nutrition-studies/raw_anonymized_data.csv";
            let file_path = "data/Nutrition.csv";
            let directory = "data";
            match extract(url, file_path, directory) {
                Ok(path) => println!("Data successfully written to: {}", path),
                Err(e) => eprintln!("An error occurred: {}", e),
            }
        }
        "transform_load" => {
            let dataset_path = "data/Nutrition_subset.csv";
            if let Err(e) = transform_load(dataset_path) {
                eprintln!("An error occurred during transform_load: {}", e);
            }
        }
        "crud" => {
            if let Err(e) = run_crud_operations() {
                eprintln!("An error occurred during CRUD operations: {}", e);
            }
        }
        "query" => {
            // Run the queries individually
            if let Err(e) = query_frequent_soda() {
                eprintln!("An error occurred during query_frequent_soda: {}", e);
            }
            if let Err(e) = query_heart_disease() {
                eprintln!("An error occurred during query_heart_disease: {}", e);
            }
        }
        "find_user" => {
            if args.len() < 3 {
                eprintln!("Please provide a user ID for find_user.");
                return;
            }
            // Parse the user ID and call the query function
            if let Ok(user_id) = args[2].parse::<i32>() {
                if let Err(e) = find_user_by_id(user_id) {
                    eprintln!("An error occurred during user query: {}", e);
                }
            } else {
                eprintln!("Invalid user ID provided.");
            }
        }

        _ => eprintln!("Invalid argument. Use 'extract', 'transform_load', 'crud', or 'query'."),
    }
}
