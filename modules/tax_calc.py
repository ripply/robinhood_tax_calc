from .database import establish_connection

def calculate_dividends_interest(cursor, tax_year):
    """Calculate total dividends and interest."""
    sql = f"""
        SELECT SUM(Amount) 
        FROM transactions 
        WHERE trans_code IN ('INT', 'IADJ', 'CDIV', 'MDIV')
        AND activity_date >= '{tax_year}-01-01'
        AND activity_date <= '{tax_year}-12-31'
    """
    cursor.execute(sql)

    result = cursor.fetchone()
    total = result[0] if result[0] else 0
    return total

def calculate_total_fees(cursor, tax_year):
    """Calculate total fees paid to robinhood since year (Gold, Margin, ADR fee and Foreign tax)"""
    sql = f"""
        SELECT SUM(Amount) 
        FROM transactions 
        WHERE trans_code IN ('GOLD', 'MINT', 'AFEE', 'DFEE', 'DTAX')
        AND activity_date >= '{tax_year}-01-01'
        AND activity_date <= '{tax_year}-12-31'
    """
    cursor.execute(sql)

    result = cursor.fetchone()
    total = result[0] if result[0] else 0
    return total

def calculate_total_investment(cursor, tax_year):
    """Calculate total investment since year"""
    sql = f"""
        SELECT SUM(Amount) 
        FROM transactions 
        WHERE trans_code IN ('ACH')
        AND activity_date >= '{tax_year}-01-01'
        AND activity_date <= '{tax_year}-12-31'
    """
    cursor.execute(sql)

    result = cursor.fetchone()
    total = result[0] if result[0] else 0
    return total

def calculate_stock_gains_and_losses(cursor, tax_year):
    """Calculate capital gains and losses from stock trades."""
    cursor.execute("""
        SELECT settle_date, instrument, cumulative_factor
        FROM splits
        ORDER BY settle_date, instrument
    """)

    splits = {}
    for row in cursor.fetchall():
        settle_date, instrument, cumulative_factor = row
        if instrument not in splits:
            holdings[instrument] = {}
        splits[instrument][settle_date] = cumulative_factor

    print(splits)
        
    cursor.execute("""
        SELECT settle_date, instrument, trans_code, quantity, amount
        FROM transactions
        WHERE trans_code IN ('Buy', 'BCXL', 'Sell')
        ORDER BY activity_date, process_date, settle_date
    """)

    holdings = {}  # To hold the cost basis and quantity of each stock
    total_gain_loss = 0.0  # To hold the total gain or loss

    for row in cursor.fetchall():
        settle_date, instrument, trans_code, quantity, amount = row
        # Cast quantity to int because it is stored as text
        quantity = float(quantity)
        # split correction
        if settle_date >= splits[instrument]['settle_date']:
            quantity = quantity * splits[instrument]['cumulative_factor']
        
        if trans_code in ('Buy'):
            # Add to holdings
            if instrument not in holdings:
                holdings[instrument] = {'cost_basis': 0.0, 'quantity': 0}
            # Amount is negative for 'Buy'
            holdings[instrument]['cost_basis'] -= amount
            holdings[instrument]['quantity'] += quantity

        elif trans_code in ('Sell', 'BCXL'):
            try:
                # Calculate gain or loss
                avg_purchase_price = holdings[instrument]['cost_basis'] / \
                    holdings[instrument]['quantity']
                gain_loss = amount - avg_purchase_price * \
                    quantity  # Amount is positive for 'Sell'

                # Update holdings
                holdings[instrument]['cost_basis'] -= avg_purchase_price * quantity
                holdings[instrument]['quantity'] -= quantity
            except KeyError:
                print(
                    f"Cannot sell a stock that doesn't exist: " +
                    f"Sold {quantity} {instrument} on {settle_date.split('T')[0]} for ${amount}")

            # Adjust gain (loss) if the closing transaction occured this year
            if settle_date[:4] == str(tax_year):
                total_gain_loss += gain_loss

    return total_gain_loss


def calculate_options_gains_and_losses(cursor, tax_year):
    cursor.execute("""
        SELECT activity_date, description, trans_code, quantity, amount, instrument
        FROM transactions
        WHERE trans_code IN ('BTC', 'BTO', 'STC', 'STO', 'OEXP')
        ORDER BY activity_date, process_date, settle_date,
        CASE WHEN trans_code IN ('BTO', 'STO') THEN 0 ELSE 1 END
    """)

    open_positions = {}
    closed_positions = {}
    total_gain_or_loss = 0

    for row in cursor.fetchall():
        activity_date, description, trans_code, quantity, amount, instrument = row

        # Clean option expiration description and quantity
        if trans_code == 'OEXP':
            description = description.replace("Option Expiration for ", "")
            if quantity[-1] == 'S':
                # Remove the last character from the quantity
                quantity = quantity[:-1]
        # Cast quantity to int because it is stored as text
        quantity = int(quantity)

        # Handle opening transactions
        if trans_code in ('BTO', 'STO'):
            if description not in open_positions:
                open_positions[description] = {'cost': 0.0, 'quantity': 0}
            # Amount is negative for 'Buy'
            open_positions[description]['quantity'] += quantity
            open_positions[description]['cost'] -= amount
      
        
        # Handle closing transactions
        elif trans_code in ('STC', 'BTC', 'OEXP'):
            if description in open_positions:
                avg_cost = open_positions[description]['cost'] / \
                    open_positions[description]['quantity']
                total_cost = avg_cost * quantity

                # Add gain (loss) only if the closing transaction occured this year
                if activity_date[:4] == str(tax_year):
                    gain_or_loss = amount - total_cost
                    total_gain_or_loss += gain_or_loss

                    # Add instrument to closed positions and add gain (loss)
                    if instrument not in closed_positions:
                        closed_positions[instrument] = {'gain_loss': 0.0}
                    closed_positions[instrument]['gain_loss'] += gain_or_loss

                # Update open positions
                open_positions[description]['quantity'] -= quantity
                open_positions[description]['cost'] -= total_cost

            # Ignore if there is no matching open position and the position closed last year
            elif activity_date[:4] == str(tax_year):
                # Throw an error and quit the program
                print(f"Error: Cannot close an option that doesn't exist.")
                raise SystemExit
            
    # Sort closed positions by gain (loss)
    closed_positions = {k: v for k, v in sorted(
        closed_positions.items(), key=lambda item: item[1]['gain_loss'], reverse=True)}
    
    # Print closed positions formatted .2f
    print("Closed Positions order by profitability:")
    for instrument, data in closed_positions.items():
        print(f"{instrument}: ${data['gain_loss']:.2f}")

    return total_gain_or_loss


if __name__ == "__main__":
    conn, cursor = establish_connection('transactions.sqlite')
    conn.commit()
    conn.close()
