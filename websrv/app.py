import psycopg2
from flask import Flask
app = Flask(__name__)


@app.route("/portfolios/<portfolio_id>")
def get_price(portfolio_id):
    conn = psycopg2.connect(
        database="saifudb",
        user="saifudb",
        password="saifudb",
        host="saifudb")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT balance,
                   quote_time
              FROM saifu_portfolio_historical_prices
             WHERE portfolio_id = %s
               AND currency = 'USD'
          ORDER BY quote_time DESC
             LIMIT 1
        """, (portfolio_id))
        row = cur.fetchone()
        conn.commit()
        return """
            <h3>Balance</h3>
            <p>
              <strong>{}</strong> USD (As of: {})
            </p>
        """.format(row[0], row[1])

def main():
    app.run(debug=True, host='0.0.0.0')

if __name__ == '__main__':
    main()
