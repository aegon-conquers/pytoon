from machine import Pin, UART, Timer
import time

# Initialize UART for DY-HV20T communication (for sound playback)
uart = UART(0, baudrate=9600, tx=Pin(0), rx=Pin(1))

# Initialize PIR motion sensor on GPIO15 (VOUT connected to this pin)
pir_sensor = Pin(15, Pin.IN)

# Initialize LEDs for thunder effect
led1 = Pin(2, Pin.OUT)
led2 = Pin(3, Pin.OUT)
led3 = Pin(4, Pin.OUT)

# Function to send command to DY-HV20T
def send_command(command):
    uart.write(command)
    time.sleep(0.1)

# Function to calculate checksum
def calculate_checksum(command):
    return sum(command) & 0xFF

# Function to play a specific track (e.g., thunder sound)
def play_track(track_number):
    track_high_byte = (track_number >> 8) & 0xFF  # High byte
    track_low_byte = track_number & 0xFF           # Low byte
    
    # Construct the command to play the track
    command = bytearray([0xAA, 0x07, 0x02, track_high_byte, track_low_byte])
    
    # Calculate checksum
    checksum = calculate_checksum(command)
    command.append(checksum)
    
    # Send command via UART
    send_command(command)
    print(f"Playing track {track_number:05d}.mp3")

# Thunder effect with LEDs (random flickering)
def thunder_effect(timer):
    led1.toggle()
    time.sleep(0.05)
    led2.toggle()
    time.sleep(0.1)
    led3.toggle()
    time.sleep(0.05)
    led1.toggle()
    led2.toggle()
    time.sleep(0.05)
    led3.toggle()

# Motion detected handler
def motion_detected():
    if pir_sensor.value() == 1:  # Motion detected (VOUT goes HIGH)
        print("Motion detected! Activating sound and thunder effect.")
        
        # Play a spooky sound (track 1 for example)
        play_track(1)
        
        # Simulate thunder flickering effect with LEDs
        thunder_timer = Timer()
        thunder_timer.init(period=100, mode=Timer.PERIODIC, callback=thunder_effect)
        
        # Stop flickering after 3 seconds
        time.sleep(3)
        thunder_timer.deinit()
        
        # Turn off all LEDs after the effect
        led1.off()
        led2.off()
        led3.off()

# Main loop
while True:
    motion_detected()  # Continuously check for motion
    time.sleep(0.1)    # Small delay to avoid high CPU usage
