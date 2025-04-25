
#!/usr/bin/env python3
import os
import time
import gi
import info_pb2
from google.protobuf.timestamp_pb2 import Timestamp
import hashlib
import struct

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

# --- Configuration ---
IMAGE_DIR_RIGHT     = "/home/ivm/escargot/imgs_right"
IMAGE_DIR_LEFT      = "/home/ivm/escargot/imgs_left"
PATTERN             = "img%05d.jpg"
FPS                 = 4
# Two separate SRT URIs for left and right
VIDEO_SRT_URI_LEFT  = "srt://127.0.0.1:6020?mode=listener"
VIDEO_SRT_URI_RIGHT = "srt://127.0.0.1:6021?mode=listener"
TCP_HOST            = "127.0.0.1"
TCP_PORT            = 7000

# MD5("StreamInfo")
_key = hashlib.md5(b"StreamInfo").digest()
# Nanoseconds per frame
frame_duration = Gst.SECOND // FPS

# --- Internal counters for each side ---
video_indexes = {IMAGE_DIR_LEFT: 1, IMAGE_DIR_RIGHT: 1}
meta_index    = 1

# --- Video callback generator ---
def on_need_data_video(appsrc, length, image_dir):
    """
    Generic callback for video frames. Pushes JPEG images from image_dir.
    """
    idx = video_indexes[image_dir]
    path = os.path.join(image_dir, PATTERN % idx)

    if not os.path.exists(path):
        appsrc.emit('end-of-stream')
        return

    # Read and push image data
    with open(path, 'rb') as f:
        data = f.read()

    buf = Gst.Buffer.new_allocate(None, len(data), None)
    buf.fill(0, data)
    buf.pts = (idx - 1) * frame_duration
    buf.duration = frame_duration
    appsrc.emit('push-buffer', buf)

    video_indexes[image_dir] += 1

# --- Metadata callback ---
def on_need_data_meta(appsrc, length):
    global meta_index
    # Push one KLV packet
    info = info_pb2.StreamInfo()
    info.filename = f"{meta_index:05d}.jpg"
    now = time.time()
    ts = Timestamp(seconds=int(now), nanos=int((now - int(now)) * 1e9))
    info.systemtime.CopyFrom(ts)
    info.session_name = "Session Offline"
    payload = info.SerializeToString()

    frame = bytearray(_key)
    frame += struct.pack(">I", len(payload))
    frame += payload
    buf = Gst.Buffer.new_wrapped(frame)
    buf.pts = (meta_index - 1) * frame_duration
    buf.duration = frame_duration
    appsrc.emit('push-buffer', buf)

    meta_index += 1

# --- Message handler ---
def on_message(bus, message, loop):
    if message.type == Gst.MessageType.ERROR:
        err, dbg = message.parse_error()
        print(f"[ERROR] {err.message}")
        if dbg:
            print("Debug:", dbg)
        loop.quit()
    elif message.type == Gst.MessageType.EOS:
        print("[EOS] End of stream")
        loop.quit()

# --- Main ---
def main():
    Gst.init(None)

    # Left video pipeline (SRT)
    pipeline_left = Gst.parse_launch(
        f"appsrc name=video_left caps=\"image/jpeg,framerate={FPS}/1\" is-live=true block=true format=time ! "
        "decodebin ! videoconvert ! video/x-raw,format=I420 ! jpegenc ! rtpjpegpay mtu=1316 ! "
        f"srtserversink uri={VIDEO_SRT_URI_LEFT}"
    )
    src_left = pipeline_left.get_by_name('video_left')
    src_left.connect('need-data', on_need_data_video, IMAGE_DIR_LEFT)

    # Right video pipeline (SRT)
    pipeline_right = Gst.parse_launch(
        f"appsrc name=video_right caps=\"image/jpeg,framerate={FPS}/1\" is-live=true block=true format=time ! "
        "decodebin ! videoconvert ! video/x-raw,format=I420 ! jpegenc ! rtpjpegpay mtu=1316 ! "
        f"srtserversink uri={VIDEO_SRT_URI_RIGHT}"
    )
    src_right = pipeline_right.get_by_name('video_right')
    src_right.connect('need-data', on_need_data_video, IMAGE_DIR_RIGHT)

    # Metadata pipeline (KLV over MPEG-TS via TCP)
    pipeline_meta = Gst.parse_launch(
        f"appsrc name=klvsrc caps=\"meta/x-klv,parsed=true,framerate={FPS}/1\" is-live=true block=true format=time ! "
        "queue ! mpegtsmux ! tcpserversink "
        f"host={TCP_HOST} port={TCP_PORT} sync=true"
    )
    klvsrc = pipeline_meta.get_by_name('klvsrc')
    klvsrc.connect('need-data', on_need_data_meta)

    # Main loop and bus setup
    loop = GLib.MainLoop()
    for pipeline in (pipeline_left, pipeline_right, pipeline_meta):
        bus = pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect('message', lambda b, msg: on_message(b, msg, loop))

    # Start streaming
    pipeline_left.set_state(Gst.State.PLAYING)
    pipeline_right.set_state(Gst.State.PLAYING)
    pipeline_meta.set_state(Gst.State.PLAYING)
    print(f"Streaming LEFT on {VIDEO_SRT_URI_LEFT} and RIGHT on {VIDEO_SRT_URI_RIGHT} & metadata on tcp://{TCP_HOST}:{TCP_PORT} at {FPS} FPS")

    try:
        loop.run()
    except KeyboardInterrupt:
        print("Interrupted")
    finally:
        for pipeline in (pipeline_left, pipeline_right, pipeline_meta):
            pipeline.set_state(Gst.State.NULL)


if __name__ == '__main__':
    main()
