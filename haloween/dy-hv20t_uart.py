from machine import UART, Pin
import time
import random

# Initialize UART for communication with DY-HV20T
uart = UART(0, baudrate=9600, tx=Pin(0), rx=Pin(1))

# Function to send command to DY-HV20T
def send_command(command):
    uart.write(command)
    time.sleep(0.1)

# Function to generate the UART command to play a specific track
def play_random_track():
    # Generate a random track number between 1 and 1000
    track_number = random.randint(1, 1000)
    
    # Convert the track number to two hexadecimal bytes (S.N.H and S.N.L)
    track_high_byte = (track_number >> 8) & 0xFF  # High byte
    track_low_byte = track_number & 0xFF           # Low byte
    
    # Construct the command to play the track
    # Command format: AA 07 02 S.N.H S.N.L SM (SM is checksum)
    command = bytearray([0xAA, 0x07, 0x02, track_high_byte, track_low_byte])
    
    # Calculate the checksum (sum of all bytes, mask it with 0xFF to keep the lower byte)
    checksum = sum(command) & 0xFF
    command.append(checksum)
    
    # Send the command via UART
    send_command(command)
    
    print(f"Playing track {track_number}...")

# Example: Play a random track
play_random_track()

# Keep playing random tracks every 30 seconds
while True:
    play_random_track()
    time.sleep(30)  # Wait for 30 seconds before playing another random track
