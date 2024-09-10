from machine import UART, Pin
import time

# Initialize UART for communication with DY-HV20T
uart = UART(0, baudrate=9600, tx=Pin(0), rx=Pin(1))

# Function to send command to DY-HV20T
def send_command(command):
    uart.write(command)
    time.sleep(0.1)

# Command to play the first track (Track 1)
play_track_1 = b'\xAA\x07\x02\x00\x01\xAC'  # Play track 1
send_command(play_track_1)

# Command to pause playback
pause_command = b'\xAA\x03\x00\xAD'
time.sleep(10)  # Play for 10 seconds, then pause
send_command(pause_command)

# Command to stop playback
stop_command = b'\xAA\x04\x00\xAE'
time.sleep(5)  # Pause for 5 seconds, then stop playback
send_command(stop_command)
