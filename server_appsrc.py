
#!/usr/bin/env python3
import os
import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GObject, GLib

# --- Configuration ---
IMAGE_DIR = "/home/smith/dataset/sequences/00/image_0/jpgs"
PATTERN   = "%06d.jpg"   # ex. 000001.jpg, 000002.jpg, …
FPS       = 4
SINK_URI  = "srt://127.0.0.1:6020?mode=listener"

# Internal state for frame pushing
frame_index = 1
timestamp = 0
frame_duration = Gst.SECOND // FPS  # duration per frame in nanoseconds

# --- need-data callback for appsrc ---
def on_need_data(appsrc, length):
    global frame_index, timestamp
    # Compute file path
    filepath = os.path.join(IMAGE_DIR, PATTERN % frame_index)
    if not os.path.exists(filepath):
        # No more files: signal EOS
        appsrc.emit('end-of-stream')
        return

    # Read the JPEG data
    with open(filepath, 'rb') as f:
        data = f.read()

    # Create a Gst.Buffer wrapping the JPEG bytes
    buf = Gst.Buffer.new_allocate(None, len(data), None)
    buf.fill(0, data)
    buf.pts = timestamp
    buf.duration = frame_duration

    # Push buffer into the pipeline
    retval = appsrc.emit('push-buffer', buf)
    if retval != Gst.FlowReturn.OK:
        print(f"[WARN] push-buffer returned {retval}")

    # Advance to next frame
    timestamp += frame_duration
    frame_index += 1

# --- Bus message handler ---
def on_message(bus, message, loop):
    t = message.type
    if t == Gst.MessageType.ERROR:
        err, dbg = message.parse_error()
        print(f"[ERROR] {err.message}")
        if dbg:
            print("Debug info:", dbg)
        loop.quit()
    elif t == Gst.MessageType.EOS:
        print("[EOS] End-Of-Stream reached")
        loop.quit()

# --- Main function ---
def main():
    GObject.threads_init()
    Gst.init(None)

    # Build pipeline with appsrc
    pipeline_desc = (
        "appsrc name=mysrc caps=\"image/jpeg,framerate=%d/1\" "
        "is-live=true format=time ! "
        "decodebin ! videoconvert ! video/x-raw,format=I420 ! "
        "jpegenc ! jpegparse ! rtpjpegpay mtu=1316 ! "
        f"srtserversink uri={SINK_URI}"
    ) % FPS

    pipeline = Gst.parse_launch(pipeline_desc)
    appsrc = pipeline.get_by_name('mysrc')
    appsrc.set_property('format', Gst.Format.TIME)
    appsrc.connect('need-data', on_need_data)

    # Configure bus to catch errors and EOS
    bus = pipeline.get_bus()
    loop = GLib.MainLoop()
    bus.add_signal_watch()
    bus.connect('message', on_message, loop)

    # Start playback
    pipeline.set_state(Gst.State.PLAYING)
    print("Pipeline running with appsrc… Ctrl+C to stop.")
    try:
        loop.run()
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        pipeline.set_state(Gst.State.NULL)

if __name__ == '__main__':
    main()
