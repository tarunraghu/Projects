import subprocess
import time
import os
import sys
import signal
import psutil
from datetime import datetime

class FlaskAppManager:
    def __init__(self):
        self.processes = {}
        self.base_dir = r"C:\Users\tarun\Downloads\tarunraghu.github.io\Projects\MediPriceInsight Project"
        self.apps = [
            {
                "name": "Main Application",
                "port": 5000,
                "directory": self.base_dir,
                "pid": None
            },
            {
                "name": "Type 1 CSV Direct Ingestion",
                "port": 5001,
                "directory": os.path.join(self.base_dir, "MediPriceInsight Project - Type 1"),
                "pid": None
            },
            {
                "name": "Type 2 CSV Pivot Ingestion",
                "port": 5002,
                "directory": os.path.join(self.base_dir, "MediPriceInsight Project - Type 2"),
                "pid": None
            },
            {
                "name": "Type 3 JSON Direct Ingestion",
                "port": 5003,
                "directory": os.path.join(self.base_dir, "MediPriceInsight Project - Type 3"),
                "pid": None
            },
            {
                "name": "Data Dump Application",
                "port": 5004,
                "directory": os.path.join(self.base_dir, "MediPriceInsight Project - Data Dump"),
                "pid": None
            }
        ]

    def start_app(self, app):
        """Start a Flask application and store its process"""
        print(f"Starting {app['name']} on port {app['port']}...")
        try:
            process = subprocess.Popen(
                ['python', 'app.py'],
                cwd=app['directory'],
                creationflags=subprocess.CREATE_NO_WINDOW  # Run in background
            )
            app['pid'] = process.pid
            self.processes[app['pid']] = process
            time.sleep(2)  # Give some time for the app to start
            print(f"✓ {app['name']} started successfully (PID: {process.pid})")
        except Exception as e:
            print(f"✗ Failed to start {app['name']}: {str(e)}")

    def stop_app(self, app):
        """Stop a Flask application"""
        if app['pid'] and app['pid'] in self.processes:
            try:
                process = self.processes[app['pid']]
                process.terminate()
                process.wait(timeout=5)
                print(f"✓ {app['name']} stopped successfully")
            except Exception as e:
                print(f"✗ Failed to stop {app['name']}: {str(e)}")
            finally:
                app['pid'] = None
                del self.processes[process.pid]

    def start_all(self):
        """Start all applications"""
        print("\n=== Starting MediPriceInsight Applications ===")
        for app in self.apps:
            self.start_app(app)
        print("\nAll applications have been started!")
        self.show_status()

    def stop_all(self):
        """Stop all applications"""
        print("\n=== Stopping MediPriceInsight Applications ===")
        for app in self.apps:
            self.stop_app(app)
        print("\nAll applications have been stopped!")

    def show_status(self):
        """Show the status of all applications"""
        print("\n=== Application Status ===")
        print("You can access the applications at:")
        for app in self.apps:
            status = "Running" if app['pid'] else "Stopped"
            print(f"- {app['name']}: http://localhost:{app['port']} ({status})")
        print("\nCommands:")
        print("1. 'status' - Show current status")
        print("2. 'restart' - Restart all applications")
        print("3. 'stop' - Stop all applications")
        print("4. 'exit' - Stop all applications and exit")
        print("5. 'help' - Show this help message")

    def handle_command(self, command):
        """Handle user commands"""
        command = command.lower().strip()
        if command == 'status':
            self.show_status()
        elif command == 'restart':
            self.stop_all()
            self.start_all()
        elif command == 'stop':
            self.stop_all()
        elif command == 'exit':
            self.stop_all()
            return False
        elif command == 'help':
            self.show_status()
        else:
            print("Unknown command. Type 'help' for available commands.")
        return True

def main():
    manager = FlaskAppManager()
    
    # Set up signal handler for graceful shutdown
    def signal_handler(sig, frame):
        print("\nShutting down gracefully...")
        manager.stop_all()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start all applications
    manager.start_all()
    
    # Command loop
    running = True
    while running:
        try:
            command = input("\nEnter command (type 'help' for options): ")
            running = manager.handle_command(command)
        except KeyboardInterrupt:
            running = False
        except Exception as e:
            print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 