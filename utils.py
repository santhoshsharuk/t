import os
import re
import sys # Added for PyInstaller path handling
import barcode
from barcode.writer import ImageWriter
from datetime import datetime, timedelta, UTC # Import UTC
import sqlite3 # Added to handle sqlite3.Error in get_sales_analytics

# --- PyInstaller Path Helper ---
def _resource_path_utils(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller, relative to utils.py location for dev. """
    try:
        base_path = sys._MEIPASS
    except AttributeError: 
        base_path = os.path.dirname(os.path.abspath(__file__)) 
    return os.path.join(base_path, relative_path)

def generate_barcode(product_code: str, output_filename_base: str):
    if not product_code:
        raise ValueError("Product code cannot be empty for barcode generation.")
    if not output_filename_base:
        raise ValueError("Output filename base cannot be empty for barcode generation.")
    try:
        output_dir = os.path.dirname(output_filename_base)
        os.makedirs(output_dir, exist_ok=True)
        code128 = barcode.get('code128', str(product_code), writer=ImageWriter())
        saved_path = code128.save(output_filename_base)
        print(f"Barcode image saved to: {saved_path}")
        return saved_path
    except Exception as e:
        print(f"Error generating or saving barcode for code '{product_code}' at '{output_filename_base}': {e}")
        raise

def get_custom_range_analytics(conn, from_date_str: str, to_date_str: str):
    """
    Get detailed sales analytics for a custom date range.
    """
    if conn is None:
        return None # Indicate error or no connection
    if not from_date_str or not to_date_str:
        return None # Invalid date range

    cur = conn.cursor()
    analytics = {}

    try:
        # 1. Total Gross Sales
        cur.execute("""
            SELECT SUM(s.quantity * s.sale_price)
            FROM sales s
            WHERE DATE(s.sale_date) BETWEEN ? AND ?
        """, (from_date_str, to_date_str))
        analytics['total_gross_sales'] = cur.fetchone()[0] or 0.0

        # 2. Total Discounts Applied on Bills
        # Sum distinct discount_applied_to_bill per bill_identifier
        cur.execute("""
            SELECT SUM(unique_discounts.discount_amount)
            FROM (
                SELECT DISTINCT s.bill_identifier, s.discount_applied_to_bill AS discount_amount
                FROM sales s
                WHERE DATE(s.sale_date) BETWEEN ? AND ? AND s.discount_applied_to_bill > 0
            ) unique_discounts
        """, (from_date_str, to_date_str))
        analytics['total_discounts_on_bills'] = cur.fetchone()[0] or 0.0
        
        # 3. Total Value of Returns
        cur.execute("""
            SELECT SUM(r.quantity * r.return_price)
            FROM returns r
            WHERE DATE(r.return_date) BETWEEN ? AND ?
        """, (from_date_str, to_date_str))
        analytics['total_returns_value'] = cur.fetchone()[0] or 0.0

        # 4. Net Sales
        analytics['total_net_sales'] = analytics['total_gross_sales'] - analytics['total_discounts_on_bills'] - analytics['total_returns_value']

        # 5. Number of Bills (Transactions)
        cur.execute("""
            SELECT COUNT(DISTINCT s.bill_identifier)
            FROM sales s
            WHERE DATE(s.sale_date) BETWEEN ? AND ?
        """, (from_date_str, to_date_str))
        analytics['number_of_bills'] = cur.fetchone()[0] or 0

        # 6. Total Items Sold (Gross Quantity)
        cur.execute("""
            SELECT SUM(s.quantity)
            FROM sales s
            WHERE DATE(s.sale_date) BETWEEN ? AND ?
        """, (from_date_str, to_date_str))
        analytics['total_items_sold_gross_qty'] = cur.fetchone()[0] or 0

        # 7. Average Items per Bill
        analytics['avg_items_per_bill'] = (analytics['total_items_sold_gross_qty'] / analytics['number_of_bills']) if analytics['number_of_bills'] > 0 else 0.0
        
        # 8. Average Bill Value (Net)
        analytics['avg_bill_value_net'] = (analytics['total_net_sales'] / analytics['number_of_bills']) if analytics['number_of_bills'] > 0 else 0.0

        # --- Profit Calculation ---
        # 9.a. Total Cost of Gross Sales (using current cost_price)
        cur.execute("""
            SELECT SUM(s.quantity * p.cost_price)
            FROM sales s
            JOIN products p ON s.product_id = p.id
            WHERE DATE(s.sale_date) BETWEEN ? AND ?
        """, (from_date_str, to_date_str))
        total_cost_of_gross_sales = cur.fetchone()[0] or 0.0
        analytics['total_cost_of_gross_sales'] = total_cost_of_gross_sales

        # 9.b. Total Cost of Returned Goods (using current cost_price)
        cur.execute("""
            SELECT SUM(r.quantity * p.cost_price)
            FROM returns r
            JOIN products p ON r.product_id = p.id
            WHERE DATE(r.return_date) BETWEEN ? AND ?
        """, (from_date_str, to_date_str))
        total_cost_of_returned_goods = cur.fetchone()[0] or 0.0
        analytics['total_cost_of_returned_goods'] = total_cost_of_returned_goods

        # 9.c. Total Net Cost of Goods Sold
        analytics['total_net_cogs'] = total_cost_of_gross_sales - total_cost_of_returned_goods
        analytics['estimated_total_profit'] = analytics['total_net_sales'] - analytics['total_net_cogs']

        # 9. Top 5 Selling Products (by net quantity: sold_qty - returned_qty)
        cur.execute("""
            SELECT 
                p.id, p.name,
                SUM(s.quantity) as gross_sold_qty,
                (SELECT COALESCE(SUM(r.quantity), 0) FROM returns r WHERE r.product_id = p.id AND DATE(r.return_date) BETWEEN :from_date AND :to_date) as returned_qty
            FROM sales s
            JOIN products p ON s.product_id = p.id
            WHERE DATE(s.sale_date) BETWEEN :from_date AND :to_date
            GROUP BY p.id, p.name
            ORDER BY (SUM(s.quantity) - (SELECT COALESCE(SUM(r.quantity), 0) FROM returns r WHERE r.product_id = p.id AND DATE(r.return_date) BETWEEN :from_date AND :to_date)) DESC
            LIMIT 5
        """, {"from_date": from_date_str, "to_date": to_date_str})
        top_sellers_by_qty_raw = cur.fetchall()
        analytics['top_sellers_by_net_qty'] = []
        for row in top_sellers_by_qty_raw:
            net_sold_qty = (row['gross_sold_qty'] or 0) - (row['returned_qty'] or 0)
            analytics['top_sellers_by_net_qty'].append({
                'id': row['id'], 'name': row['name'], 'net_sold_qty': net_sold_qty, 'gross_sold_qty': row['gross_sold_qty'], 'returned_qty': row['returned_qty']
            })
        
        # 10. Top 5 Selling Products (by net revenue: (sold_qty * sale_price) - (returned_qty * return_price_avg_for_product)
        # This is complex due to varying sale/return prices. Simpler: Top by gross revenue, then show net units.
        cur.execute("""
            SELECT 
                p.id, p.name,
                SUM(s.quantity * s.sale_price) as gross_revenue,
                SUM(s.quantity) as gross_sold_qty,
                (SELECT COALESCE(SUM(r.quantity), 0) FROM returns r WHERE r.product_id = p.id AND DATE(r.return_date) BETWEEN :from_date AND :to_date) as returned_qty
            FROM sales s
            JOIN products p ON s.product_id = p.id
            WHERE DATE(s.sale_date) BETWEEN :from_date AND :to_date
            GROUP BY p.id, p.name
            ORDER BY gross_revenue DESC
            LIMIT 5
        """, {"from_date": from_date_str, "to_date": to_date_str})
        top_sellers_by_revenue_raw = cur.fetchall()
        analytics['top_sellers_by_gross_revenue'] = []
        for row in top_sellers_by_revenue_raw:
             analytics['top_sellers_by_gross_revenue'].append({
                'id': row['id'], 'name': row['name'], 'gross_revenue': row['gross_revenue'] or 0.0,
                'gross_sold_qty': row['gross_sold_qty'] or 0, 'returned_qty': row['returned_qty'] or 0
            })
        
        return analytics

    except sqlite3.Error as e:
        print(f"Database error during custom range analytics: {e}")
        return {"error": str(e)} # Return error info
    except Exception as e:
        print(f"Unexpected error during custom range analytics: {e}")
        import traceback
        traceback.print_exc()
        return {"error": "An unexpected error occurred."}


def get_sales_analytics(conn, period='all'):
    """Get sales analytics for different time periods, including returns and product summary."""
    if conn is None:
        print("Error: Database connection is None in get_sales_analytics.")
        return [] 

    cur = conn.cursor()
    today_utc = datetime.now(UTC).date()
    results = []

    try:
        if period == 'daily':
            for i in range(7):
                date_obj = today_utc - timedelta(days=i)
                date_str = date_obj.strftime('%Y-%m-%d')
                cur.execute("SELECT SUM(s.quantity * s.sale_price) FROM sales s WHERE DATE(s.sale_date) = ?", (date_str,))
                gross_sales = cur.fetchone()[0] or 0.0
                
                # Sum distinct discount_applied_to_bill per bill_identifier for the day
                cur.execute("""
                    SELECT SUM(unique_discounts.discount_amount)
                    FROM (
                        SELECT DISTINCT s.bill_identifier, s.discount_applied_to_bill AS discount_amount
                        FROM sales s
                        WHERE DATE(s.sale_date) = ? AND s.discount_applied_to_bill > 0
                    ) unique_discounts
                """, (date_str,))
                total_discounts_on_bills = cur.fetchone()[0] or 0.0

                cur.execute("SELECT SUM(r.quantity * r.return_price) FROM returns r WHERE DATE(r.return_date) = ?", (date_str,))
                total_returns_value = cur.fetchone()[0] or 0.0
                
                net_sales = gross_sales - total_discounts_on_bills - total_returns_value
                results.append({
                    'date': date_str, 'total_sales': round(gross_sales, 2), # This is Gross Sales
                    'total_discounts_on_bills': round(total_discounts_on_bills, 2),
                    'total_returns_value': round(total_returns_value, 2), 
                    'net_sales': round(net_sales, 2)
                })
        elif period == 'weekly':
            for i in range(4):
                ref_date = today_utc - timedelta(weeks=i)
                start_date = ref_date - timedelta(days=ref_date.isoweekday() - 1)
                end_date = start_date + timedelta(days=6)
                start_date_str, end_date_str = start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
                cur.execute("SELECT SUM(s.quantity * s.sale_price) FROM sales s WHERE DATE(s.sale_date) BETWEEN ? AND ?", (start_date_str, end_date_str,))
                gross_sales = cur.fetchone()[0] or 0.0

                cur.execute("""
                    SELECT SUM(unique_discounts.discount_amount)
                    FROM (
                        SELECT DISTINCT s.bill_identifier, s.discount_applied_to_bill AS discount_amount
                        FROM sales s
                        WHERE DATE(s.sale_date) BETWEEN ? AND ? AND s.discount_applied_to_bill > 0
                    ) unique_discounts
                """, (start_date_str, end_date_str))
                total_discounts_on_bills = cur.fetchone()[0] or 0.0

                cur.execute("SELECT SUM(r.quantity * r.return_price) FROM returns r WHERE DATE(r.return_date) BETWEEN ? AND ?", (start_date_str, end_date_str,))
                total_returns_value = cur.fetchone()[0] or 0.0
                
                net_sales = gross_sales - total_discounts_on_bills - total_returns_value
                results.append({
                    'week': start_date_str, 'week_start': start_date_str, 'week_end': end_date_str,
                    'total_sales': round(gross_sales, 2), 
                    'total_discounts_on_bills': round(total_discounts_on_bills, 2),
                    'total_returns_value': round(total_returns_value, 2),
                    'net_sales': round(net_sales, 2)
                })
        elif period == 'monthly':
            current_year, current_month = today_utc.year, today_utc.month
            for i in range(12):
                year, month = current_year, current_month - i
                while month <= 0: month += 12; year -= 1
                month_str, year_str = str(month).zfill(2), str(year)
                cur.execute("SELECT SUM(s.quantity * s.sale_price) FROM sales s WHERE strftime('%Y', s.sale_date) = ? AND strftime('%m', s.sale_date) = ?", (year_str, month_str,))
                gross_sales = cur.fetchone()[0] or 0.0

                cur.execute("""
                    SELECT SUM(unique_discounts.discount_amount)
                    FROM (
                        SELECT DISTINCT s.bill_identifier, s.discount_applied_to_bill AS discount_amount
                        FROM sales s
                        WHERE strftime('%Y', s.sale_date) = ? AND strftime('%m', s.sale_date) = ? AND s.discount_applied_to_bill > 0
                    ) unique_discounts
                """, (year_str, month_str))
                total_discounts_on_bills = cur.fetchone()[0] or 0.0

                cur.execute("SELECT SUM(r.quantity * r.return_price) FROM returns r WHERE strftime('%Y', r.return_date) = ? AND strftime('%m', r.return_date) = ?", (year_str, month_str,))
                total_returns_value = cur.fetchone()[0] or 0.0
                
                net_sales = gross_sales - total_discounts_on_bills - total_returns_value
                results.append({
                    'month': f"{year_str}-{month_str}", 'total_sales': round(gross_sales, 2),
                    'total_discounts_on_bills': round(total_discounts_on_bills, 2),
                    'total_returns_value': round(total_returns_value, 2), 
                    'net_sales': round(net_sales, 2)
                })
        elif period == 'yearly':
            cur.execute("SELECT strftime('%Y', s.sale_date) as year, SUM(s.quantity * s.sale_price) as gross_sales_yearly FROM sales s WHERE s.sale_date IS NOT NULL GROUP BY year ORDER BY year DESC")
            fetched_gross_sales = cur.fetchall()
            
            cur.execute("""
                SELECT strftime('%Y', s.sale_date) as year, SUM(unique_discounts.discount_amount) as total_discount_yearly
                FROM (
                    SELECT DISTINCT s.bill_identifier, s.discount_applied_to_bill AS discount_amount, strftime('%Y', s.sale_date) as sale_year
                    FROM sales s
                    WHERE s.sale_date IS NOT NULL AND s.discount_applied_to_bill > 0
                ) unique_discounts
                GROUP BY sale_year ORDER BY sale_year DESC
            """)
            fetched_discounts = {row['year']: row['total_discount_yearly'] for row in cur.fetchall() if row['year']}

            cur.execute("SELECT strftime('%Y', r.return_date) as year, SUM(r.quantity * r.return_price) as total_returns_yearly FROM returns r WHERE r.return_date IS NOT NULL GROUP BY year ORDER BY year DESC")
            fetched_returns = {row['year']: row['total_returns_yearly'] for row in cur.fetchall() if row['year']}
            
            for row_gross in fetched_gross_sales:
                year_val = row_gross['year']
                if year_val:
                    gross_sales = row_gross['gross_sales_yearly'] or 0.0
                    total_discounts_on_bills = fetched_discounts.get(year_val, 0.0)
                    total_returns_value = fetched_returns.get(year_val, 0.0)
                    net_sales = gross_sales - total_discounts_on_bills - total_returns_value
                    results.append({
                        'year': year_val, 'total_sales': round(gross_sales, 2),
                        'total_discounts_on_bills': round(total_discounts_on_bills, 2),
                        'total_returns_value': round(total_returns_value, 2), 
                        'net_sales': round(net_sales, 2)
                    })
        elif period == 'best_sellers': # Based on gross units sold
            cur.execute("""
                SELECT p.id, p.name, SUM(s.quantity) as total_sold,
                       COALESCE(p.selling_price, 0.0) as selling_price,
                       COALESCE(p.cost_price, 0.0) as cost_price
                FROM sales s JOIN products p ON s.product_id = p.id
                GROUP BY p.id, p.name, p.selling_price, p.cost_price
                ORDER BY total_sold DESC LIMIT 5
            """)
            for row in cur.fetchall():
                profit = (row[3] - row[4]) * row[2] if row[3] is not None and row[4] is not None and row[2] is not None else 0.0
                results.append({
                    'id': row[0], 'name': row[1], 'total_sold': row[2] or 0,
                    'selling_price': row[3], 'cost_price': row[4], 'profit': round(profit, 2)
                })
        elif period == 'unsold':
            cur.execute("""
                SELECT p.id, p.name, p.quantity, COALESCE(p.selling_price, 0.0) as selling_price,
                       COALESCE(p.cost_price, 0.0) as cost_price, c.name as category_name
                FROM products p LEFT JOIN categories c ON p.category_id = c.id
                WHERE p.id NOT IN (SELECT DISTINCT product_id FROM sales)
                ORDER BY p.name COLLATE NOCASE
            """)
            for row in cur.fetchall():
                results.append({
                    'id': row[0], 'name': row[1], 'quantity': row[2] or 0,
                    'selling_price': row[3], 'cost_price': row[4],
                    'category': row[5] if row[5] else 'Uncategorized'
                })
        elif period == 'product_summary':
            cur.execute("""
                SELECT
                    p.id, p.name, p.barcode, p.cost_price, p.selling_price,
                    p.quantity as current_stock,
                    COALESCE(c.name, 'Uncategorized') as category_name,
                    SUM(CASE WHEN s.id IS NOT NULL THEN s.quantity ELSE 0 END) as total_units_sold_gross,
                    SUM(CASE WHEN s.id IS NOT NULL THEN s.quantity * s.sale_price ELSE 0 END) as total_revenue_generated
                FROM products p
                LEFT JOIN categories c ON p.category_id = c.id
                LEFT JOIN sales s ON p.id = s.product_id
                GROUP BY p.id, p.name, p.barcode, p.cost_price, p.selling_price, p.quantity, c.name
                ORDER BY p.name COLLATE NOCASE
            """)
            fetched_products = cur.fetchall()
            for row_dict in fetched_products: # cur.fetchall() already gives list of dicts if row_factory is sqlite3.Row
                row = dict(row_dict) # Ensure it's a mutable dict if needed
                cost_price = row.get('cost_price', 0.0) or 0.0
                selling_price = row.get('selling_price', 0.0) or 0.0
                total_units_sold_gross = row.get('total_units_sold_gross', 0) or 0
                total_revenue = row.get('total_revenue_generated', 0.0) or 0.0
                
                total_cost_of_goods_sold = cost_price * total_units_sold_gross
                profit_gross = total_revenue - total_cost_of_goods_sold
                
                cur.execute("SELECT SUM(r.quantity) FROM returns r WHERE r.product_id = ?", (row['id'],))
                total_units_returned = cur.fetchone()[0] or 0
                net_units_sold = total_units_sold_gross - total_units_returned

                results.append({
                    'id': row['id'], 'name': row['name'], 'barcode': row['barcode'],
                    'category_name': row['category_name'], 'cost_price': round(cost_price, 2),
                    'selling_price': round(selling_price, 2), 'current_stock': row['current_stock'],
                    'total_units_sold_gross': total_units_sold_gross,
                    'total_units_returned': total_units_returned,
                    'net_units_sold': net_units_sold,
                    'total_revenue_generated': round(total_revenue, 2),
                    'profit_generated_gross': round(profit_gross, 2)
                })

    except sqlite3.Error as e:
        print(f"Database error during analytics query for period '{period}': {e}")
        return [] 
    except Exception as e: 
        print(f"Unexpected error during analytics generation for period '{period}': {e}")
        import traceback
        traceback.print_exc() 
        return [] 

    return results