import re
from datetime import datetime
from collections import OrderedDict
import matplotlib.pyplot as plt
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage


# Define a class to hold table properties
class TableProperties:
    def __init__(self):
        self.createTime = None
        self.endTime = None
        self.noOfRecords = None
        self.timeTaken = None

# Function to parse the log file and extract table properties
def parse_log(log_file):
    table_map = OrderedDict()  # Store table properties in an ordered map
    current_table = None
    current_timestamp = None

    # Read the log file line by line
    with open(log_file, 'r') as f:
        for line in f:
            table_start_match = re.search(r'hivevar:tablename=([^>]+)', line)
            if table_start_match:
                current_table = table_start_match.group(1)
                if current_table not in table_map:
                    table_map[current_table] = TableProperties()

            timestamp_match = re.search(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', line)
            if timestamp_match:
                current_timestamp = datetime.strptime(timestamp_match.group(), '%Y-%m-%d %H:%M:%S')
                if current_table and not table_map[current_table].createTime:
                    table_map[current_table].createTime = current_timestamp

            table_end_match = re.search(r'Completion: (\d+) Rows loaded to spendiq.([^>]+)', line)
            if table_end_match and current_table:
                table_map[current_table].endTime = current_timestamp
                table_map[current_table].noOfRecords = int(table_end_match.group(1))
                time_taken = (table_map[current_table].endTime - table_map[current_table].createTime).total_seconds() / 60
                table_map[current_table].timeTaken = time_taken

    return table_map

# Replace 'path/to/your/log/file.txt' with the actual log file path
log_file = 'path/to/your/log/file.txt'
table_properties_map = parse_log(log_file)

# Extract data for plotting
table_names = []
start_times = []
time_taken = []
record_counts = []

# Sort the tables by time taken (descending order) and limit to top N
limit = 10  # Set the number of top tables you want to display
sorted_tables = sorted(table_properties_map.items(), key=lambda x: x[1].timeTaken, reverse=True)[:limit]

for table, properties in sorted_tables:
    table_names.append(table)
    start_times.append(properties.startTime)
    time_taken.append(properties.timeTaken)
    record_counts.append(properties.noOfRecords)  # Store the record counts

# Normalize the time_taken values between 0 and 1 for gradient coloring
normalized_times = np.array(time_taken) / max(time_taken)

# Create a gradient of colors from green to red
colors = plt.cm.RdYlGn(normalized_times)

# Create a bar graph with gradient coloring
plt.figure(figsize=(10, 6))
bars = plt.barh(table_names[::-1], time_taken[::-1], color=colors)  # Reverse the lists to order from bottom to top

# Annotate the bars with record counts
for bar, count in zip(bars, record_counts[::-1]):
    plt.text(bar.get_width(), bar.get_y() + bar.get_height()/2, f'{count:,}', va='center')

plt.xlabel('Time Taken (minutes)')
plt.ylabel('Table Names')
plt.title(f'Top {limit} Time Taken and Record Count for Each Table')
plt.tight_layout()

# Save the plot as an image file
image_file = 'top_tables_plot.png'
plt.savefig(image_file)

# Send an email with the plot attached
email_subject = 'Top Tables Analysis'
email_body = 'Attached is the plot showing the top tables analysis.'
recipient_email = 'recipient@example.com'  # Replace with the recipient's email address

msg = MIMEMultipart()
msg['From'] = 'sender@example.com'  # Replace with the sender's email address
msg['To'] = recipient_email
msg['Subject'] = email_subject
msg.attach(MIMEText(email_body))

# Attach the image to the email
with open(image_file, 'rb') as img:
    image = MIMEImage(img.read())
    image.add_header('Content-Disposition', 'attachment', filename=image_file)
    msg.attach(image)

# Set up the email server and send the email
smtp_server = 'smtp.example.com'
smtp_port = 587
smtp_username = 'your_username'
smtp_password = 'your_password'

with smtplib.SMTP(smtp_server, smtp_port) as server:
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.sendmail(msg['From'], recipient_email, msg.as_string())

print('Email sent successfully.')

# Display the bar graph
plt.show()



# Save the plot as an image file
image_file = 'top_tables_plot.png'
plt.savefig(image_file)

# Send an email with the plot attached
email_subject = 'Top Tables Analysis'
email_body = 'Attached is the plot showing the top tables analysis.'
recipient_email = 'recipient@example.com'  # Replace with the recipient's email address

msg = MIMEMultipart()
msg['From'] = 'sender@example.com'  # Replace with the sender's email address
msg['To'] = recipient_email
msg['Subject'] = email_subject
msg.attach(MIMEText(email_body))

# Attach the image to the email
with open(image_file, 'rb') as img:
    image = MIMEImage(img.read())
    image.add_header('Content-Disposition', 'attachment', filename=image_file)
    msg.attach(image)

# Set up the email server and send the email
smtp_server = 'smtp.example.com'
smtp_port = 587
smtp_username = 'your_username'
smtp_password = 'your_password'

with smtplib.SMTP(smtp_server, smtp_port) as server:
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.sendmail(msg['From'], recipient_email, msg.as_string())

print('Email sent successfully.')

# Display the bar graph
plt.show()
