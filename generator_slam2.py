#!/usr/bin/env python#!/usr/bin/env python3
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
# IMAGE_DIR_RIGHT     = "/home/smith/dataset/sequences/00/image_0/jpgs_numbered"
# IMAGE_DIR_LEFT      = "/home/smith/dataset/sequences/00/image_1/jpgs_numbered"

# SLAM config
IMAGE_DIR_RIGHT = "/home/ivm/escargot/imgs_right_numbered"
IMAGE_DIR_LEFT  = "/home/ivm/escargot/imgs_left_numbered"

# File naming pattern
PATTERN = "img%05d.jpg"

# Streaming parameters
FPS                 = 4
VIDEO_SRT_URI_LEFT  = "srt://127.0.0.1:6020?mode=listener"
VIDEO_SRT_URI_RIGHT = "srt://127.0.0.1:6021?mode=listener"
TCP_HOST            = "127.0.0.1"
TCP_PORT            = 7000

# Initialize GStreamer and indexes
Gst.init(None)
_key = hashlib.md5(b"StreamInfo").digest()
frame_duration = Gst.SECOND // FPS
video_indexes = {IMAGE_DIR_LEFT: 1, IMAGE_DIR_RIGHT: 1}
meta_indexes  = {IMAGE_DIR_LEFT: 1, IMAGE_DIR_RIGHT: 1}

# Store PTS values per frame index
frame_pts = {}

def on_need_data_video(appsrc, length, image_dir):
    idx = video_indexes[image_dir]
    path = os.path.join(image_dir, PATTERN % idx)
    if not os.path.exists(path):
        appsrc.emit('end-of-stream')
        return
    with open(path, 'rb') as f:
        data = f.read()
    buf = Gst.Buffer.new_allocate(None, len(data), None)
    buf.fill(0, data)
    buf.pts = (idx - 1) * frame_duration
    buf.duration = frame_duration
    appsrc.emit('push-buffer', buf)
    video_indexes[image_dir] += 1

def make_meta_callback(image_dir):
    def on_need_data_meta(appsrc, length):
        idx = meta_indexes[image_dir]
        info = info_pb2.StreamInfo()
        info.filename = os.path.join(image_dir, PATTERN % idx)
        now = time.time()
        ts = Timestamp(seconds=int(now), nanos=int((now - int(now)) * 1e9))
        info.systemtime.CopyFrom(ts)
        info.session_name = "Session Offline"
        payload = info.SerializeToString()

        frame = bytearray(_key) + struct.pack(">I", len(payload)) + payload
        buf = Gst.Buffer.new_wrapped(frame)
        buf.pts = (idx - 1) * frame_duration
        buf.duration = frame_duration
        appsrc.emit('push-buffer', buf)
        meta_indexes[image_dir] += 1
    return on_need_data_meta

def on_message(bus, message, loop):
    if message.type == Gst.MessageType.ERROR:
        err, dbg = message.parse_error()
        print(f"[ERROR] {err.message}")
        if dbg: print("Debug:", dbg)
        loop.quit()
    elif message.type == Gst.MessageType.EOS:
        print("[EOS] End of stream")
        loop.quit()

def main():
    # Build pipeline
    pipeline_desc = (
        # Video left
        f"appsrc name=vid_left caps=\"image/jpeg,framerate={FPS}/1\" is-live=true block=true format=time ! "
        "decodebin ! videoconvert ! video/x-raw,format=I420 ! jpegenc name=chk1 ! rtpjpegpay mtu=1316 ! "
        f"srtserversink latency=1000 max-lateness=10000000000 uri={VIDEO_SRT_URI_LEFT} "

        # Video right
        f"appsrc name=vid_right caps=\"image/jpeg,framerate={FPS}/1\" is-live=true block=true format=time ! "
        "decodebin ! videoconvert ! video/x-raw,format=I420 ! jpegenc name=chk2 ! rtpjpegpay mtu=1316 ! "
        f"srtserversink latency=1000 max-lateness=10000000000 uri={VIDEO_SRT_URI_RIGHT} "

        # Metadata left with pacing by PTS
        f"appsrc name=klv_left caps=\"meta/x-klv,parsed=true,framerate={FPS}/1\" is-live=true block=true format=time ! "
        "queue ! mpegtsmux name=mux ! "
        f"tcpserversink host={TCP_HOST} port={TCP_PORT} sync=true "

        # Metadata right
        f"appsrc name=klv_right caps=\"meta/x-klv,parsed=true,framerate={FPS}/1\" is-live=true block=true format=time ! "
        "queue ! mux."
    )

    print("generator pipeline :", pipeline_desc)
    pipeline = Gst.parse_launch(pipeline_desc)

    # Connect callbacks for video streams
    pipeline.get_by_name('vid_left').connect('need-data', on_need_data_video, IMAGE_DIR_LEFT)
    pipeline.get_by_name('vid_right').connect('need-data', on_need_data_video, IMAGE_DIR_RIGHT)

    # Connect callbacks for metadata streams
    pipeline.get_by_name('klv_left').connect('need-data', make_meta_callback(IMAGE_DIR_LEFT))
    pipeline.get_by_name('klv_right').connect('need-data', make_meta_callback(IMAGE_DIR_RIGHT))

    # Helper to record and display 4 PTS per frame
    def pad_probe_callback(pad, info, name):
        buf = info.get_buffer()
        if buf:
            pts_sec = buf.pts / Gst.SECOND
            frame_idx = int(buf.pts / frame_duration) + 1
            if frame_idx not in frame_pts:
                frame_pts[frame_idx] = {}
            frame_pts[frame_idx][name] = pts_sec
            if len(frame_pts[frame_idx]) == 4:
                pts = frame_pts.pop(frame_idx)
                print(f"[FRAME {frame_idx}] chk1_out={pts.get('chk1_out'):.3f}s, chk2_out={pts.get('chk2_out'):.3f}s, klv_left={pts.get('klv_left'):.3f}s, klv_right={pts.get('klv_right'):.3f}s")
        return Gst.PadProbeReturn.OK

    # Attach probes to pads
    for name in ['chk1', 'chk2']:
        elem = pipeline.get_by_name(name)
        pad = elem.get_static_pad('src')
        pad.add_probe(Gst.PadProbeType.BUFFER, lambda pad, info, n=name: pad_probe_callback(pad, info, f"{n}_out"))
    for n in ['klv_left', 'klv_right']:
        elem = pipeline.get_by_name(n)
        pad = elem.get_static_pad('src')
        pad.add_probe(Gst.PadProbeType.BUFFER, lambda pad, info, n=n: pad_probe_callback(pad, info, n))

    # Bus and loop
    loop = GLib.MainLoop()
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect('message', lambda b, m: on_message(b, m, loop))

    pipeline.set_state(Gst.State.PLAYING)
    print(f"Streaming LEFT→{VIDEO_SRT_URI_LEFT}, RIGHT→{VIDEO_SRT_URI_RIGHT}, KLV→tcp://{TCP_HOST}:{TCP_PORT} @ {FPS} FPS")

    try:
        loop.run()
    except KeyboardInterrupt:
        print("Interrupted")
    finally:
        pipeline.set_state(Gst.State.NULL)

if __name__ == '__main__':
    main()

