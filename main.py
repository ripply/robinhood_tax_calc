# main.py

from modules.database import establish_connection
from modules.tax_calc import calculate_dividends_interest, calculate_stock_gains_and_losses

TAX_YEAR = 2023

def main():
    conn, cursor = establish_connection('transactions.sqlite')
    
    dividends_and_interest = calculate_dividends_interest(cursor, TAX_YEAR)
    stock_gains_and_losses = calculate_stock_gains_and_losses(cursor, TAX_YEAR)
    # option_gains_and_losses = calculate_option_gains_and_losses(cursor)
    
    print(f"Total dividends and interest: ${dividends_and_interest}")
    print(f"Total stock gains and losses: ${stock_gains_and_losses}")
    # print(f"Total option gains and losses: ${option_gains_and_losses}")

if __name__ == "__main__":
    main()
