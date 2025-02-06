from .database import establish_connection

from collections import defaultdict, deque
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Deque

@dataclass
class Lot:
    quantity: float
    price: float
    date: datetime.date
    is_option: bool
    wash_sale_adjustment: float = 0.0
    total_cost: float = 0.0  # Track total cost of the lot

    def __post_init__(self):
        self.total_cost = self.quantity * self.price

@dataclass
class Transaction:
    date: datetime.date
    settle_date: str
    instrument: str
    trans_type: str
    quantity: float
    amount: float
    
    @property
    def is_option(self) -> bool:
        return self.trans_type in ('BTO', 'STC')
    
    @property
    def is_buy(self) -> bool:
        return self.trans_type in ('Buy', 'BTO')
    
    @property
    def is_sell(self) -> bool:
        return self.trans_type in ('Sell', 'STC')

def calculate_stock_gains_and_losses(cursor, tax_year: int) -> float:
    """Calculate capital gains/losses with correct wash sale handling."""
    
    cursor.execute("""
        SELECT activity_date, settle_date, instrument, trans_code, quantity, amount
        FROM transactions
        WHERE trans_code IN ('Buy', 'BCXL', 'Sell', 'BTO', 'STC') and instrument in ('AAPL')
        ORDER BY activity_date, process_date, settle_date, -row
    """)
    
    def parse_date(date_string: str) -> datetime.date:
        return datetime.fromisoformat(date_string).date()
    
    transactions: List[Transaction] = [
        Transaction(
            date=parse_date(row[0]),
            settle_date=row[1],
            instrument=row[2],
            trans_type=row[3],
            quantity=float(row[4]),
            amount=float(row[5])
        )
        for row in cursor.fetchall()
    ]
    
    holdings: Dict[str, Dict[bool, Deque[Lot]]] = defaultdict(lambda: {
        True: deque(),   # Options
        False: deque()   # Stocks
    })
    
    pending_wash_sales: Dict[str, List[tuple]] = defaultdict(list)
    
    # Track gains and losses by instrument
    realized_gains: Dict[str, float] = defaultdict(float)
    realized_losses: Dict[str, float] = defaultdict(float)
    disallowed_losses: Dict[str, float] = defaultdict(float)
    
    def find_replacement_shares(trans: Transaction, window_start: datetime.date, 
                              window_end: datetime.date) -> bool:
        return any(
            t.date > trans.date and t.date <= window_end and
            t.is_buy and t.is_option == trans.is_option and
            t.instrument == trans.instrument
            for t in transactions
        )
    
    for trans in transactions:
        print(trans)
        if trans.is_buy:
            # Calculate total cost for the lot
            total_cost = abs(trans.amount)
            price_per_unit = total_cost / trans.quantity
            
            new_lot = Lot(
                quantity=trans.quantity,
                price=price_per_unit,
                date=trans.date,
                is_option=trans.is_option,
                total_cost=total_cost
            )
            holdings[trans.instrument][trans.is_option].append(new_lot)
            
        elif trans.is_sell:
            remaining_to_sell = trans.quantity
            realized_gain_loss = 0.0
            sale_proceeds = abs(trans.amount)
            price_per_unit = sale_proceeds / trans.quantity
            
            while remaining_to_sell > 0 and holdings[trans.instrument][trans.is_option]:
                lot = holdings[trans.instrument][trans.is_option][0]
                sell_quantity = min(remaining_to_sell, lot.quantity)
                
                # Calculate gain/loss based on total amounts
                lot_cost_basis = (lot.price * sell_quantity)
                sale_amount = price_per_unit * sell_quantity
                gain_loss = sale_amount - lot_cost_basis
                
                print(f"  -> Selling {sell_quantity} from lot ({lot.quantity} @ ${lot.price:.2f} from {lot.date})")
                print(f"     Sale amount: ${sale_amount:.2f}, Cost basis: ${lot_cost_basis:.2f}")
                print(f"     Gain/Loss: ${gain_loss:.2f}")
                
                if gain_loss < 0:
                    window_start = trans.date
                    window_end = trans.date + timedelta(days=30)
                    
                    if find_replacement_shares(trans, window_start, window_end):
                        print(f"  🔴 Wash Sale: Loss of ${-gain_loss:.2f} deferred")
                        disallowed_losses[trans.instrument] += -gain_loss
                        gain_loss = 0  # Disallow the loss
                    else:
                        print(f"  ✅ Regular Loss: ${gain_loss:.2f} realized")
                        realized_losses[trans.instrument] += gain_loss
                else:
                    realized_gains[trans.instrument] += gain_loss
                
                # Update or remove the lot
                if lot.quantity > sell_quantity:
                    lot.quantity -= sell_quantity
                    lot.total_cost = lot.quantity * lot.price
                else:
                    holdings[trans.instrument][trans.is_option].popleft()
                    
                remaining_to_sell -= sell_quantity
                realized_gain_loss += gain_loss
    
    print("\n🔹 Final Summary 🔹")
    total_gain_loss = 0
    
    for instrument in set(t.instrument for t in transactions):
        print(f"\n{instrument} Summary:")
        
        # Print remaining positions
        for is_option, lots in holdings[instrument].items():
            if lots:
                type_str = "Options" if is_option else "Stocks"
                total_quantity = sum(lot.quantity for lot in lots)
                total_cost_basis = sum(lot.total_cost for lot in lots)
                print(f"  {type_str}:")
                print(f"    - Final Quantity: {total_quantity}")
                print(f"    - Final Cost Basis: ${total_cost_basis:.2f}")
        
        # Print trading summary
        print(f"  Trading Summary:")
        print(f"    - Total Realized Gains: ${realized_gains[instrument]:.2f}")
        print(f"    - Total Realized Losses: ${realized_losses[instrument]:.2f}")
        print(f"    - Disallowed Losses (Wash Sales): ${disallowed_losses[instrument]:.2f}")
        net_gain_loss = realized_gains[instrument] + realized_losses[instrument]
        print(f"    - Net Realized Gain/Loss: ${net_gain_loss:.2f}")
        print(f"    - Running Total: ${total_gain_loss:.2f}")
        
        total_gain_loss += net_gain_loss
    
    print(f"\n🔹 Total Capital Gain/Loss for {tax_year}: ${total_gain_loss:.2f}\n")
    
    return total_gain_loss

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
