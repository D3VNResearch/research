
# PIPELINE IMPORT DATA OF SAVILLS

## Introduction
This project is designed to automate the process of importing data for Savills. The pipeline handles data extraction, transformation, and loading (ETL) from various sources into a centralized database, ensuring data integrity and consistency.

## Requirements
Before running the code, ensure you have the following installed:

- Python 3.11+
- pip newest version
- Required packages (listed in `requirements.txt`)

## Installation
To install the necessary packages, run:

```bash
pip install -r requirements.txt
```

Some libraries that are not written in the requirements.txt file can be installed using the syntax: 
```bash
pip install [library name]
```

All Python files use the above syntax to run.

### Note:
- The libraries may be outdated over time, so there will be warnings or bugs. Please edit the source code to suit the current environment.

## Usage
### Running the Pipeline
To execute the data import pipeline, use the following command:

```bash
python run_pipeline.py
```
## Data Flow
The data pipeline follows these steps:

1. **Extract**: Data is extracted from the specified source (e.g., CSV files, APIs, databases).
2. **Transform**: Data undergoes several transformations such as cleaning, normalization, and validation.
3. **Load**: Transformed data is loaded into the destination database.


## Contributing
If you wish to contribute to this project, please follow these steps:

1. Fork the repository
2. Create a new branch (`git checkout -b feature-branch`)
3. Commit your changes (`git commit -am 'Add new feature'`)
4. Push to the branch (`git push origin feature-branch`)
5. Create a new Pull Request

## Contact
If you have any questions or suggestions, feel free to contact [Mr. Duong] at [`duongpnh@vng.com.vn`] or ZALO [`0855525199`].
