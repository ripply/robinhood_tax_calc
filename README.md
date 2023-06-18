# Stock and Options Tax Calculator

This project is a set of scripts that will help you calculate taxes owed from stock and options trading activities using Robinhood's reports.

## Project Structure

The project has the following structure:

```bash
.
├── data
│   └── transactions.csv
├── modules
│   ├── csv_to_db.py
│   ├── database.py
│   └── tax_calc.py
├── tests
├── __init__.py
├── .gitignore
├── main.py
├── README.md
└── transactions.sqlite
```

## Description of Modules

- `database.py`: Contains functions for connecting to the SQLite database.
- `csv_to_db.py`: Converts a CSV file of transactions into a SQLite database. It will create the database, clean the data, and insert it into the database. This same moedule can be ran to insert addional data.
- `tax_calculator.py`: Contains functions for calculating taxes owed from stock and options trading activities. This includes dividends and interest as well as capital gains and losses.
- `main.py`: The main script that ties everything together.

## How to Run

1. Download report from Robinhood and copy to data directory.
2. Run the CSV to DB script to convert your CSV file of transactions into a SQLite database: `python -m modules.csv_to_db`
3. Run the main script to calculate taxes owed: `python main.py`

## Note

Please ensure your transactions CSV file follows the same format as given in the example. Any discrepancies in the CSV format may cause the scripts to fail or perform inaccurately. For example, if a stock was sold on the first of the year and purchased the previous year, the report must have the buy transaction.

## Future Enhancements

- Integrate trades with other financial statments such as balance sheet, statement of earnings, and statement of cash flow.
