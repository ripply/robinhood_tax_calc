from .database import establish_connection

def calculate_dividends_interest(cursor, tax_year):
    """Calculate total dividends and interest."""
    sql = f"""
        SELECT SUM(Amount) 
        FROM transactions 
        WHERE trans_code IN ('INT', 'CDIV')
        AND activity_date >= '{tax_year}-01-01'
    """
    cursor.execute(sql)

    result = cursor.fetchone()
    total = round(result[0] if result[0] else 0, 2)
    return total


def calculate_stock_gains_and_losses(cursor, tax_year):
    """Calculate capital gains and losses from stock trades."""
    cursor.execute("""
        SELECT settle_date, instrument, trans_code, quantity, amount
        FROM transactions
        WHERE trans_code IN ('Buy', 'Sell')
        ORDER BY activity_date, process_date, settle_date
    """)

    holdings = {}  # To hold the cost basis and quantity of each stock
    total_gain_loss = 0.0  # To hold the total gain or loss

    for row in cursor.fetchall():
        settle_date, instrument, trans_code, quantity, amount = row
        # Cast quantity to int because it is stored as text
        quantity = float(quantity)
        if trans_code == 'Buy':
            # Add to holdings
            if instrument not in holdings:
                holdings[instrument] = {'cost_basis': 0.0, 'quantity': 0}
            # Amount is negative for 'Buy'
            holdings[instrument]['cost_basis'] -= amount
            holdings[instrument]['quantity'] += quantity
        elif trans_code == 'Sell' and settle_date[:4] == str(tax_year):
            # Describe the sale
            print(f"Selling {quantity} shares of {instrument} on {settle_date}")
            # Calculate gain or loss
            avg_purchase_price = holdings[instrument]['cost_basis'] / \
                holdings[instrument]['quantity']
            gain_loss = amount - avg_purchase_price * \
                quantity  # Amount is positive for 'Sell'
            total_gain_loss += gain_loss
            print(f"Gain/loss: {gain_loss:.2f}")
            # Update holdings
            holdings[instrument]['cost_basis'] -= avg_purchase_price * quantity
            holdings[instrument]['quantity'] -= quantity

    print(holdings)

    # Round to 2 decimal places
    total_gain_loss = round(total_gain_loss, 2)
    return total_gain_loss


def calculate_options_gains_and_losses(cursor, tax_year):
    """Calculate capital gains and losses from options trades."""
    # Implement the calculation
    pass

if __name__ == "__main__":
    conn, cursor = establish_connection('transactions.sqlite')
    conn.commit()
    conn.close()