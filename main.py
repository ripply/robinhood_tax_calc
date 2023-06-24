# main.py

from modules.database import establish_connection
from modules.tax_calc import calculate_total_fees, calculate_total_investment, calculate_dividends_interest, calculate_stock_gains_and_losses, calculate_options_gains_and_losses

def main(tax_year):
    process_year(2018);
    process_year(2019);
    process_year(2020);
    process_year(2021);
    process_year(2022);
    process_year(2023);

def process_year(tax_year)
    conn, cursor = establish_connection('transactions.sqlite')

    fees_paid = calculate_total_fees(cursor, tax_year)
    total_investment = calculate_total_investment(cursor, tax_year)
    dividends_and_interest = calculate_dividends_interest(cursor, tax_year)
    dividends_and_interest = calculate_dividends_interest(cursor, tax_year)
    stock_gains_and_losses = calculate_stock_gains_and_losses(cursor, tax_year)
    option_gains_and_losses = calculate_options_gains_and_losses(
        cursor, tax_year)
    total_gains_and_losses = stock_gains_and_losses + option_gains_and_losses + dividends_and_interest

    print(f"\nGains and losses for tax year {tax_year}")
    print(f"Total fees paid to RH: ${fees_paid:.2f}")
    print(f"Total investment: ${total_investment:.2f}")
    print(f"Total dividends and interest: ${dividends_and_interest:.2f}")
    print(f"Total stock gains and losses: ${stock_gains_and_losses:.2f}")
    print(f"Total option gains and losses: ${option_gains_and_losses:.2f}")
    print(f"Total gains and losses: ${total_gains_and_losses:.2f}")

if __name__ == "__main__":
    main()
