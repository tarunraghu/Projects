from flask import Flask, render_template, redirect
import os

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/type1')
def type1():
    # Redirect to Type 1 application
    return redirect('http://localhost:5001')

@app.route('/type2')
def type2():
    # Redirect to Type 2 application
    return redirect('http://localhost:5002')

@app.route('/type3')
def type3():
    # Redirect to Type 3 application
    return redirect('http://localhost:5003')

@app.route('/data-dump')
def data_dump():
    # Redirect to Data Dump application
    return redirect('http://localhost:5004')

@app.route('/price-transparency')
def price_transparency():
    # Redirect to Price Transparency Report application
    return redirect('http://localhost:5005')

if __name__ == '__main__':
    app.run(debug=True, port=5000, host='0.0.0.0') 