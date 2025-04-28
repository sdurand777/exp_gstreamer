
import re

klv_left = 0
video_left = 0

num_sync = 0


with open('LOGS', 'r') as f:
    for line in f:
        if line.startswith('[SYNC]'):
            num_sync += 1
            if num_sync==6:
                break
        #if '[QUEUE] Received klv left' in line:
        if '[META]' in line:
            klv_left += 1
        #elif '[QUEUE] Received frame left' in line:
        elif '[VIDEO]' in line:
            video_left += 1

print(f"KLV left count before first SYNC: {klv_left}")
print(f"Video-frame left count before first SYNC: {video_left}")
