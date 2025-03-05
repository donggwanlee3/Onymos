import random
import threading

# Maximum number of tickers and orders per side.
MAX_TICKERS = 1024
MAX_ORDERS_PER_SIDE = 1024

# Order class representing a stock order.
class Order:
    def __init__(self, order_type, ticker, quantity, price):
        self.active = True      # Order is active until matched
        self.order_type = order_type  # 0 for Buy, 1 for Sell
        self.ticker = ticker
        self.quantity = quantity
        self.price = price

# OrderBook class for each ticker.
class OrderBook:
    def __init__(self):
        # Preallocate lists of fixed size (None indicates an empty slot)
        self.buy_orders = [None] * MAX_ORDERS_PER_SIDE
        self.sell_orders = [None] * MAX_ORDERS_PER_SIDE
        self.buy_count = 0   # Next insertion index for buy orders
        self.sell_count = 0  # Next insertion index for sell orders

# Global list of order books – one for each ticker.
orderBooks = [OrderBook() for _ in range(MAX_TICKERS)]

def addOrder(order_type, ticker, quantity, price):
    """Adds an order to the appropriate order book.
    
    Parameters:
        order_type (int): 0 for Buy, 1 for Sell.
        ticker (int): Ticker symbol represented as an integer (0 to 1023).
        quantity (int): Order quantity.
        price (float): Order price.
    """
    if ticker < 0 or ticker >= MAX_TICKERS:
        return  # Invalid ticker

    book = orderBooks[ticker]
    if order_type == 0:  # Buy order
        index = book.buy_count
        if index < MAX_ORDERS_PER_SIDE:
            book.buy_orders[index] = Order(order_type, ticker, quantity, price)
            # Rely on the GIL to “atomically” update the counter.
            book.buy_count += 1
    else:  # Sell order
        index = book.sell_count
        if index < MAX_ORDERS_PER_SIDE:
            book.sell_orders[index] = Order(order_type, ticker, quantity, price)
            book.sell_count += 1

def matchOrder(ticker):
    """Matches a Buy order to the Sell order with the lowest price for the given ticker.
    
    If a Buy order with a price greater than or equal to the lowest active Sell order's price is found,
    both orders are marked as inactive and a match is printed.
    
    Time complexity is O(n) where n is the number of orders in the order book.
    """
    if ticker < 0 or ticker >= MAX_TICKERS:
        return

    book = orderBooks[ticker]
    
    # Find the lowest active Sell order price.
    lowest_sell_price = float('inf')
    lowest_sell_index = -1
    for i in range(book.sell_count):
        order = book.sell_orders[i]
        if order is not None and order.active:
            if order.price < lowest_sell_price:
                lowest_sell_price = order.price
                lowest_sell_index = i

    if lowest_sell_index == -1:
        return  # No active sell orders available

    # Scan through the Buy orders to find one that can match.
    for j in range(book.buy_count):
        order = book.buy_orders[j]
        if order is not None and order.active:
            if order.price >= lowest_sell_price:
                # Simulate atomic check-and-update (compare-and-swap) for matching.
                if order.active:
                    order.active = False  # Mark Buy order as matched
                    # Also mark the Sell order as matched.
                    sell_order = book.sell_orders[lowest_sell_index]
                    if sell_order is not None and sell_order.active:
                        sell_order.active = False
                        print(f"Matched Ticker {ticker}: Buy at {order.price} with Sell at {lowest_sell_price} (Quantity: {order.quantity})")
                        break  # Exit after matching one pair

def simulateAddOrders(numOrders):
    """Simulates adding orders with random parameters."""
    for _ in range(numOrders):
        order_type = random.randint(0, 1)          # 0 for Buy, 1 for Sell.
        ticker = random.randint(0, MAX_TICKERS - 1)  # Random ticker between 0 and 1023.
        quantity = random.randint(1, 100)            # Random quantity between 1 and 100.
        price = random.uniform(0.1, 100.0)           # Random price between 0.1 and 100.0.
        addOrder(order_type, ticker, quantity, price)

def simulateMatchOrders(numMatches):
    """Simulates matching orders by randomly selecting tickers."""
    for _ in range(numMatches):
        ticker = random.randint(0, MAX_TICKERS - 1)
        matchOrder(ticker)

if __name__ == '__main__':
    # Create threads to simulate order addition.
    num_add_threads = 4
    orders_per_thread = 1000
    add_threads = []
    for _ in range(num_add_threads):
        t = threading.Thread(target=simulateAddOrders, args=(orders_per_thread,))
        add_threads.append(t)
        t.start()

    # Create threads to simulate order matching.
    num_match_threads = 2
    matches_per_thread = 1000
    match_threads = []
    for _ in range(num_match_threads):
        t = threading.Thread(target=simulateMatchOrders, args=(matches_per_thread,))
        match_threads.append(t)
        t.start()

    # Wait for all threads to complete.
    for t in add_threads:
        t.join()
    for t in match_threads:
        t.join()