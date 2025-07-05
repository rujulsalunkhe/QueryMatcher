from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
import time

class ReloadHandler(FileSystemEventHandler):
    def __init__(self, reload_callback):
        self.reload_callback = reload_callback

    def on_modified(self, event):
        if event.src_path.endswith(('schema.json', 'templates.json')):
            print(f"🔄 Detected change in: {event.src_path}, reloading matcher...")
            self.reload_callback()

def start_watcher(reload_callback, path='.'):
    event_handler = ReloadHandler(reload_callback)
    observer = Observer()
    observer.schedule(event_handler, path=path, recursive=False)
    observer_thread = threading.Thread(target=observer.start, daemon=True)
    observer_thread.start()
