import tkinter as tk
from datetime import datetime
import pytz

# Create the main window
def create_clock_window():
    window = tk.Tk()
    window.title('Digital Clock')

    label = tk.Label(window, font=('calibri', 40, 'bold'), background='purple', foreground='white')
    label.pack(anchor='center')

    update_clock(label)
    window.mainloop()

# Update the clock information for all time zones
def update_clock(label):
    current_time_str = get_current_time_str()
    label.config(text=current_time_str)
    label.after(1000, update_clock, label)

# Get the current time in different time zones
def get_current_time_str():
    time_zones = ['UTC', 'America/New_York', 'Europe/London', 'Asia/Tokyo']
    current_times = []
    for zone in time_zones:
        tz = pytz.timezone(zone)
        current_time = datetime.now(tz)
        current_times.append(f'{zone}: {current_time.strftime('%Y-%m-%d %H:%M:%S')}')
    return '\n'.join(current_times)

if __name__ == '__main__':
    create_clock_window()