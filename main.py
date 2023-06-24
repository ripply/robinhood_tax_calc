# main.py

from modules.database import establish_connection
from modules.tax_calc import calculate_dividends_interest, calculate_stock_gains_and_losses, calculate_options_gains_and_losses

TAX_YEAR = 2020


def main():
    conn, cursor = establish_connection('transactions.sqlite')

    fees_paid = calculate_fees_paid(cursor, TAX_YEAR)
    total_investment = calculate_total_investment(cursor, TAX_YEAR)
    dividends_and_interest = calculate_dividends_interest(cursor, TAX_YEAR)
    dividends_and_interest = calculate_dividends_interest(cursor, TAX_YEAR)
    stock_gains_and_losses = calculate_stock_gains_and_losses(cursor, TAX_YEAR)
    option_gains_and_losses = calculate_options_gains_and_losses(
        cursor, TAX_YEAR)
    total_gains_and_losses = stock_gains_and_losses + option_gains_and_losses + dividends_and_interest

    print(f"\nGains and losses for tax year {TAX_YEAR}")
    print(f"Total fees paid to RH: ${fees_paid:.2f}")
    print(f"Total investment: ${total_investment:.2f}")
    print(f"Total dividends and interest: ${dividends_and_interest:.2f}")
    print(f"Total stock gains and losses: ${stock_gains_and_losses:.2f}")
    print(f"Total option gains and losses: ${option_gains_and_losses:.2f}")
    print(f"Total gains and losses: ${total_gains_and_losses:.2f}")


if __name__ == "__main__":
    main()
