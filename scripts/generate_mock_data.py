"""
Mock E-Commerce API.
Simulates a REST API for testing the ingestion pipeline.
"""
import json
import random
import uuid
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# ─── Mock Data Generation ───

CATEGORIES = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books', 'Food']
REGIONS = ['us-east', 'us-west', 'eu-west', 'eu-east', 'asia-pacific']
STATUSES = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
TIERS = ['standard', 'premium', 'vip']
CURRENCIES = ['USD', 'EUR', 'GBP']

FIRST_NAMES = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry', 'Ivy', 'Jack']
LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Wilson', 'Moore']

def generate_customers(count=500):
    customers = []
    for i in range(1, count + 1):
        customers.append({
            'customer_id': f'CUST-{i:05d}',
            'first_name': random.choice(FIRST_NAMES),
            'last_name': random.choice(LAST_NAMES),
            'email': f'customer{i}@example.com',
            'phone': f'+1-555-{random.randint(1000, 9999)}',
            'region': random.choice(REGIONS),
            'country': random.choice(['US', 'UK', 'CA', 'DE', 'FR']),
            'tier': random.choice(TIERS),
            'created_at': (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 500))).isoformat(),
        })
    return customers

def generate_products(count=100):
    products = []
    for i in range(1, count + 1):
        category = random.choice(CATEGORIES)
        price = round(random.uniform(5.0, 500.0), 2)
        products.append({
            'product_id': f'PROD-{i:04d}',
            'name': f'{category} Product {i}',
            'category': category,
            'subcategory': f'{category} Sub-{random.randint(1, 5)}',
            'price': price,
            'cost': round(price * random.uniform(0.3, 0.7), 2),
            'sku': f'SKU-{category[:3].upper()}-{i:04d}',
            'status': random.choice(['active', 'inactive', 'discontinued']),
            'created_at': (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).isoformat(),
        })
    return products

def generate_orders(customers, products, count=5000):
    orders = []
    base_date = datetime(2024, 1, 1)
    
    for i in range(1, count + 1):
        customer = random.choice(customers)
        product = random.choice(products)
        quantity = random.randint(1, 5)
        amount = round(product['price'] * quantity, 2)
        status = random.choice(STATUSES)
        created = base_date + timedelta(
            days=random.randint(0, 270),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
        
        order = {
            'order_id': f'ORD-{i:06d}',
            'customer_id': customer['customer_id'],
            'product_id': product['product_id'],
            'status': status,
            'amount': amount,
            'quantity': quantity,
            'currency': 'USD',
            'created_at': created.isoformat(),
            'updated_at': (created + timedelta(hours=random.randint(1, 48))).isoformat(),
        }
        
        if status in ['shipped', 'delivered']:
            order['shipped_at'] = (created + timedelta(hours=random.randint(2, 24))).isoformat()
        if status == 'delivered':
            order['delivered_at'] = (created + timedelta(days=random.randint(1, 7))).isoformat()
        
        orders.append(order)
    
    return sorted(orders, key=lambda x: x['created_at'])

# ─── Generate All Data ───
CUSTOMERS = generate_customers()
PRODUCTS = generate_products()
ORDERS = generate_orders(CUSTOMERS, PRODUCTS)
EVENTS = [
    {
        'event_id': f'evt-{i:08d}',
        'event_type': random.choice(['order_created', 'order_shipped', 'payment', 'refund']),
        'source': 'webhook',
        'timestamp': (datetime(2024, 1, 1) + timedelta(hours=random.randint(0, 6480))).isoformat(),
        'payload': {'order_id': random.choice(ORDERS)['order_id']},
    }
    for i in range(1, 2001)
]

DATA_STORE = {
    '/orders': ORDERS,
    '/products': PRODUCTS,
    '/customers': CUSTOMERS,
    '/events': EVENTS,
}


class MockAPIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)
        
        if path not in DATA_STORE:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(json.dumps({'error': 'Not found'}).encode())
            return
        
        data = DATA_STORE[path]
        
        # Pagination
        offset = int(params.get('offset', [0])[0])
        limit = int(params.get('limit', [100])[0])
        
        paginated = data[offset:offset + limit]
        
        response = {
            'data': paginated,
            'total': len(data),
            'offset': offset,
            'limit': limit,
        }
        
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response, default=str).encode())
    
    def log_message(self, format, *args):
        pass  # Suppress logs


if __name__ == '__main__':
    port = 8001
    server = HTTPServer(('0.0.0.0', port), MockAPIHandler)
    print(f'Mock API running on port {port}')
    print(f'  Orders: {len(ORDERS)}')
    print(f'  Products: {len(PRODUCTS)}')
    print(f'  Customers: {len(CUSTOMERS)}')
    print(f'  Events: {len(EVENTS)}')
    server.serve_forever()
