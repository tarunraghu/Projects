from flask import Flask, render_template, redirect, url_for
import os
import subprocess

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/type1')
def type1():
    # Path to Type 1 project directory
    type1_dir = r"C:\Users\tarun\Downloads\tarunraghu.github.io\Projects\MediPriceInsight Project\MediPriceInsight Project - Type 1"
    
    # Start the Type 1 Flask application in a new process
    subprocess.Popen(['python', 'app.py'], 
                    cwd=type1_dir, 
                    creationflags=subprocess.CREATE_NEW_CONSOLE)
    
    # Redirect to the Type 1 application
    return redirect('http://localhost:5001')

@app.route('/type2')
def type2():
    # Path to Type 2 project directory
    type2_dir = r"C:\Users\tarun\Downloads\tarunraghu.github.io\Projects\MediPriceInsight Project\MediPriceInsight Project - Type 2"
    
    # Start the Type 2 Flask application in a new process
    subprocess.Popen(['python', 'app.py'], 
                    cwd=type2_dir, 
                    creationflags=subprocess.CREATE_NEW_CONSOLE)
    
    # Redirect to the Type 2 application
    return redirect('http://localhost:5002')

@app.route('/type3')
def type3():
    # Path to Type 3 project directory
    type3_dir = r"C:\Users\tarun\Downloads\tarunraghu.github.io\Projects\MediPriceInsight Project\MediPriceInsight Project - Type 3"
    
    # Start the Type 3 Flask application in a new process
    subprocess.Popen(['python', 'app.py'], 
                    cwd=type3_dir, 
                    creationflags=subprocess.CREATE_NEW_CONSOLE)
    
    # Redirect to the Type 3 application
    return redirect('http://localhost:5003')

@app.route('/dump-data')
def data_dump():
    # Path to Request Data Dump application directory
    data_dump_dir = r"C:\Users\tarun\Downloads\tarunraghu.github.io\Projects\MediPriceInsight Project\Request Data Dump"
    
    # Start the Request Data Dump Flask application in a new process
    subprocess.Popen(['python', 'app.py'], 
                    cwd=data_dump_dir, 
                    creationflags=subprocess.CREATE_NEW_CONSOLE)
    
    # Redirect to the Request Data Dump application
    return redirect('http://127.0.0.1:5004')

if __name__ == '__main__':
    app.run(debug=True, port=5000)