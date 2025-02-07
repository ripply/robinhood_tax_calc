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
    total_cost: float = 0.0  # calculated as quantity * price

    def __post_init__(self):
        self.total_cost = self.quantity * self.price

@dataclass
class Transaction:
    date: datetime.date
    settle_date: str
    instrument: str
    trans_type: str
    quantity: float
    quantity_str: str
    amount: float
    description: str

    @property
    def is_option(self) -> bool:
        # In this context BTO, STC, and OEXP are considered option trades.
        return self.trans_type in ('BTO', 'STC', 'OEXP')

    @property
    def is_buy(self) -> bool:
        return self.trans_type in ('Buy', 'BTO')

    @property
    def is_sell(self) -> bool:
        return self.trans_type in ('Sell', 'STC')

def get_key(trans: Transaction) -> tuple:
    """
    Returns a key used to segregate lots and wash sale adjustments.
    For options (including expirations), use (instrument, description) so that 
    options on different strikes (or expiration dates) are handled separately.
    For stocks, use (instrument, False).

    For OEXP transactions, remove the "Option Expiration for " prefix so that
    the key matches the description used for the original options.
    """
    if trans.is_option:
        desc = trans.description
        if trans.trans_type == 'OEXP':
            prefix = "Option Expiration for "
            if desc.startswith(prefix):
                desc = desc[len(prefix):].strip()
        return (trans.instrument, desc)
    else:
        return (trans.instrument, False)

def calculate_stock_gains_and_losses(cursor, tax_year: int) -> float:
    """
    Calculate capital gains/losses with aggregated wash sale handling.
    
    In this version:
      • When a sell occurs, we remove shares from FIFO holdings until the sale
        quantity is filled, and then compute one overall net gain/loss for that sale.
      • If the sale results in a loss, we defer the loss only for the number
        of shares that will be replaced within 30 days.
      • Option expiration events (trans_code "OEXP") are processed similarly to sells,
        except that if no quantity is provided, all open options for that strike are expired.
      
    Options are keyed using (instrument, description) so that different strikes/expirations
    are processed separately.
    """
    # Include OEXP in the query.
    cursor.execute("""
        SELECT activity_date, settle_date, instrument, trans_code, quantity, amount, description
        FROM transactions
        WHERE trans_code IN ('Buy', 'BCXL', 'Sell', 'BTO', 'STC', 'OEXP')
        ORDER BY activity_date, process_date, settle_date, -row
    """)

    def parse_date(date_string: str) -> datetime.date:
        return datetime.fromisoformat(date_string).date()

    # For the quantity field, if it is empty (as is the case for OEXP), use 0.0.
    transactions: List[Transaction] = [
        Transaction(
            date=parse_date(row[0]),
            settle_date=row[1],
            instrument=row[2],
            trans_type=row[3],
            quantity=0.0,
            quantity_str=row[4],
            amount=float(row[5]),
            description=row[6]
        )
        for row in cursor.fetchall()
    ]

    for trans in transactions:
        try:
            trans.quantity = float(trans.quantity_str)
        except ValueError:
            trans.quantity = 0.0

    # Dictionary keyed by (instrument, identifier)
    holdings: Dict[tuple, Deque[Lot]] = defaultdict(deque)
    # pending_wash_sales holds deferred loss adjustments keyed in the same way.
    pending_wash_sales: Dict[tuple, List[dict]] = defaultdict(list)
    # Track realized gains and realized losses (losses not deferred)
    realized_gains: Dict[str, float] = defaultdict(float)
    realized_losses: Dict[str, float] = defaultdict(float)
    # (For reporting, also track total deferred (disallowed) losses.)
    disallowed_losses: Dict[str, float] = defaultdict(float)

    # Process transactions sequentially.
    for trans in transactions:
        print(trans)
        key = get_key(trans)

        # --- Process Buys ---
        if trans.is_buy:
            total_cost = abs(trans.amount)
            adjustment = 0.0
            # Apply any pending wash sale adjustments if this buy occurs on or before their expiration.
            for pending in pending_wash_sales[key][:]:
                if trans.date <= pending['expiration']:
                    shares_to_apply = min(trans.quantity, pending['remaining_qty'])
                    adjustment += shares_to_apply * pending['loss_per_share']
                    pending['remaining_qty'] -= shares_to_apply
                    if pending['remaining_qty'] <= 0:
                        pending_wash_sales[key].remove(pending)
            total_cost += adjustment  # Increase cost basis by deferred loss adjustment.
            price_per_unit = total_cost / trans.quantity
            new_lot = Lot(
                quantity=trans.quantity,
                price=price_per_unit,
                date=trans.date,
                is_option=trans.is_option
            )
            holdings[key].append(new_lot)
            print(f"Buy on {trans.date}: Quantity {trans.quantity}, Amount {abs(trans.amount)}, Adjustment {adjustment}, Price per unit {price_per_unit:.2f}")

        # --- Process Sells ---
        elif trans.is_sell:
            # Calculate replacement shares among lots purchased in the 30-day window.
            current_holding_wash = sum(
                lot.quantity for lot in holdings[key]
                if (trans.date - timedelta(days=30)) <= lot.date <= trans.date
            )
            used_from_current = min(trans.quantity, current_holding_wash)
            remaining_current_replacement = current_holding_wash - used_from_current

            remaining_to_sell = trans.quantity
            sale_proceeds = abs(trans.amount)
            sale_cost_basis = 0.0
            # Remove shares from holdings using FIFO.
            while remaining_to_sell > 0 and holdings[key]:
                lot = holdings[key][0]
                sell_qty = min(remaining_to_sell, lot.quantity)
                sale_cost_basis += sell_qty * lot.price
                if lot.quantity > sell_qty:
                    lot.quantity -= sell_qty
                    lot.total_cost = lot.quantity * lot.price
                else:
                    holdings[key].popleft()
                remaining_to_sell -= sell_qty

            net_gain_loss = sale_proceeds - sale_cost_basis
            print(f"Sell on {trans.date}: Proceeds {sale_proceeds:.2f}, Cost basis {sale_cost_basis:.2f}, Net {net_gain_loss:.2f}")

            # Look ahead for future buys (after the sale) within 30 days.
            future_replacement = sum(
                t.quantity for t in transactions 
                if t.is_buy 
                and t.instrument == trans.instrument 
                and ((t.description == trans.description) if trans.is_option else (t.is_option == trans.is_option))
                and trans.date < t.date <= trans.date + timedelta(days=30)
            )
            # Total replacement shares available: unsold current shares plus future buys.
            replacement_qty = remaining_current_replacement + future_replacement

            if net_gain_loss < 0:
                if replacement_qty > 0:
                    # Defer loss only for the number of shares that are actually replaced.
                    deferred_qty = min(trans.quantity, replacement_qty)
                    loss_per_share = (-net_gain_loss) / trans.quantity
                    pending_wash_sales[key].append({
                        'remaining_qty': deferred_qty,
                        'loss_per_share': loss_per_share,
                        'expiration': trans.date + timedelta(days=30)
                    })
                    disallowed_losses[trans.instrument] += deferred_qty * loss_per_share
                    # For shares not covered by a replacement, realize the loss.
                    realized_loss_qty = trans.quantity - deferred_qty
                    if realized_loss_qty > 0:
                        realized_losses[trans.instrument] += (net_gain_loss) + (deferred_qty * loss_per_share)
                        print(f"  🔴 Partial Wash Sale: {deferred_qty} shares deferred, {realized_loss_qty} shares loss realized")
                    else:
                        print(f"  🔴 Wash Sale: Entire loss of {(-net_gain_loss):.2f} deferred at {loss_per_share:.2f} per share")
                else:
                    # No replacement shares: the entire loss is realized.
                    realized_losses[trans.instrument] += net_gain_loss
                    print(f"  ✅ Loss of {net_gain_loss:.2f} realized")
            else:
                realized_gains[trans.instrument] += net_gain_loss
                print(f"  ✅ Gain of {net_gain_loss:.2f} realized")
            print(f"---Realized losses: {realized_losses[trans.instrument]}")

        # --- Process Option Expiration ---
        elif trans.trans_type == 'OEXP':
            # For expiration events, the quantity is not provided.
            # Set trans.quantity to the total open quantity for this option strike.
            if trans.quantity == 0:
                total_open_qty = sum(lot.quantity for lot in holdings[key])
                trans.quantity = total_open_qty
                print(f"Option Expiration on {trans.date}: Expiring all open contracts for strike, total quantity set to {trans.quantity}")

            # Calculate replacement shares among lots purchased in the 30-day window.
            current_holding_wash = sum(
                lot.quantity for lot in holdings[key]
                if (trans.date - timedelta(days=30)) <= lot.date <= trans.date
            )
            used_from_current = min(trans.quantity, current_holding_wash)
            remaining_current_replacement = current_holding_wash - used_from_current

            # Look ahead for future buys (after the expiration) within 30 days.
            future_replacement = sum(
                t.quantity for t in transactions 
                if t.is_buy 
                and t.instrument == trans.instrument 
                and ((t.description == trans.description) if (t.trans_type in ('BTO', 'STC', 'OEXP')) else False)
                and trans.date < t.date <= trans.date + timedelta(days=30)
            )
            replacement_qty = remaining_current_replacement + future_replacement

            # Process the expiration like a sale: remove lots using FIFO.
            remaining_to_expire = trans.quantity
            expiration_proceeds = abs(trans.amount)
            expiration_cost_basis = 0.0
            while remaining_to_expire > 0 and holdings[key]:
                lot = holdings[key][0]
                expire_qty = min(remaining_to_expire, lot.quantity)
                expiration_cost_basis += expire_qty * lot.price
                if lot.quantity > expire_qty:
                    lot.quantity -= expire_qty
                    lot.total_cost = lot.quantity * lot.price
                else:
                    holdings[key].popleft()
                remaining_to_expire -= expire_qty

            net_gain_loss = expiration_proceeds - expiration_cost_basis
            print(f"Option Expiration on {trans.date}: Proceeds {expiration_proceeds:.2f}, Cost basis {expiration_cost_basis:.2f}, Net {net_gain_loss:.2f}")

            if net_gain_loss < 0:
                if replacement_qty > 0:
                    deferred_qty = min(trans.quantity, replacement_qty)
                    loss_per_share = (-net_gain_loss) / trans.quantity
                    pending_wash_sales[key].append({
                        'remaining_qty': deferred_qty,
                        'loss_per_share': loss_per_share,
                        'expiration': trans.date + timedelta(days=30)
                    })
                    disallowed_losses[trans.instrument] += deferred_qty * loss_per_share
                    realized_loss_qty = trans.quantity - deferred_qty
                    if realized_loss_qty > 0:
                        realized_losses[trans.instrument] += (net_gain_loss) + (deferred_qty * loss_per_share)
                        print(f"  🔴 Partial Wash Sale: {deferred_qty} contracts deferred, {realized_loss_qty} contracts loss realized")
                    else:
                        print(f"  🔴 Wash Sale: Entire loss of {(-net_gain_loss):.2f} deferred at {loss_per_share:.2f} per contract")
                else:
                    realized_losses[trans.instrument] += net_gain_loss
                    print(f"  ✅ Loss of {net_gain_loss:.2f} realized")
            else:
                realized_gains[trans.instrument] += net_gain_loss
                print(f"  ✅ Gain of {net_gain_loss:.2f} realized")
            print(f"---Realized losses: {realized_losses[trans.instrument]}")

        # (Any additional transaction types can be handled here...)

    # --- Final Summary ---
    print("\n🔹 Final Summary 🔹")
    total_gain_loss = 0.0

    # Collect holdings by instrument so we can report both stocks and options separately.
    holdings_by_instrument: Dict[str, Dict[str, Deque[Lot]]] = defaultdict(dict)
    for key, lots in holdings.items():
        instrument, ident = key
        # For stocks the ident is False; otherwise use the description string.
        bucket = ident if ident is not False else "Stocks"
        holdings_by_instrument[instrument][bucket] = lots

    for instrument in holdings_by_instrument:
        print(f"\n{instrument} Summary:")
        for bucket, lots in holdings_by_instrument[instrument].items():
            if lots:
                type_str = f"Option: {bucket}" if bucket != "Stocks" else "Stocks"
                total_qty = sum(lot.quantity for lot in lots)
                total_cost_basis = sum(lot.total_cost for lot in lots)
                print(f"  {type_str}:")
                print(f"    - Final Quantity: {total_qty}")
                print(f"    - Final Cost Basis: ${total_cost_basis:.2f}")
        print("  Trading Summary:")
        print(f"    - Total Realized Gains: ${realized_gains[instrument]:.2f}")
        print(f"    - Total Realized Losses: ${realized_losses[instrument]:.2f}")
        print(f"    - Total Deferred (Wash Sale) Losses: ${disallowed_losses[instrument]:.2f}")
        net = realized_gains[instrument] + realized_losses[instrument]
        print(f"    - Net Realized Gain/Loss: ${net:.2f}")
        total_gain_loss += net
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
