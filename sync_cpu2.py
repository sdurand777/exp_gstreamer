
#!/usr/bin/env python3
import os
import time
import gi
import info_pb2
from google.protobuf.timestamp_pb2 import Timestamp
import hashlib
import struct
import threading
import queue
import numpy as np
import cv2
from collections import deque
import math

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

# --- Configuration ---

IMAGE_DIR_RIGHT     = "/home/smith/dataset/sequences/00/image_0/jpgs_numbered"
IMAGE_DIR_LEFT      = "/home/smith/dataset/sequences/00/image_1/jpgs_numbered"
PATTERN             = "%05d.jpg"
FPS                 = 4
VIDEO_SRT_URI_LEFT  = "srt://127.0.0.1:6020?mode=caller"
VIDEO_SRT_URI_RIGHT = "srt://127.0.0.1:6021?mode=caller"
TCP_HOST            = "127.0.0.1"
TCP_PORT            = 7000
SKIP                = 3

# Initialize GStreamer and keys/indexes
Gst.init(None)
_key = hashlib.md5(b"StreamInfo").digest()

class SRTSyncClient:
    def __init__(self, fps=FPS):
        print("[INIT] Initializing SRTSyncClient")
        self.loop = GLib.MainLoop()
        self.sample_queue = queue.Queue()
        # Buffers for per-side frame/KLV
        self.frames = {'left': deque(), 'right': deque()}
        self.klvs   = {'left': deque(), 'right': deque()}
        self.global_offset = None
        # Initialize pts_precision early to avoid None
        self.pts_precision = 1.0 / FPS
        self.detected_fps = []
        self.fps_detected = False
        self.lock = threading.Lock()
        self.running = True
        self.start_pts = None

        threading.Thread(target=self._process_samples, daemon=True).start()
        self._build_pipeline()

    def _build_pipeline(self):
        desc = (
            f"srtsrc latency=1000 uri={VIDEO_SRT_URI_LEFT} ! queue !"
            "application/x-rtp,media=video,encoding-name=JPEG,payload=26 ! rtpjpegdepay name=depay_l ! jpegparse ! tee name=tee_l "
            "tee_l. ! appsink name=vid_l caps=\"image/jpeg\" emit-signals=true sync=false "
            f"srtsrc latency=1000 uri={VIDEO_SRT_URI_RIGHT} ! queue !"
            "application/x-rtp,media=video,encoding-name=JPEG,payload=26 ! rtpjpegdepay name=depay_r ! jpegparse ! tee name=tee_r "
            "tee_r. ! appsink name=vid_r caps=\"image/jpeg\" emit-signals=true sync=false "
            f"tcpclientsrc host={TCP_HOST} port={TCP_PORT} ! tsdemux name=dmx "
            "dmx. ! queue ! meta/x-klv,parsed=true ! appsink name=klv_l emit-signals=true sync=false drop=false "
            "dmx. ! queue ! meta/x-klv,parsed=true ! appsink name=klv_r emit-signals=true sync=false drop=false"
        )
        print("[PIPELINE] Launching pipeline:\n", desc)
        pipe = Gst.parse_launch(desc)
        for elem_name in ('depay_l', 'depay_r'):
            elem = pipe.get_by_name(elem_name)
            pad = elem.get_static_pad('src')
            pad.add_probe(Gst.PadProbeType.BUFFER, self._on_probe)

        pipe.get_by_name('vid_l').connect('new-sample', self._on_video, 'left')
        pipe.get_by_name('vid_r').connect('new-sample', self._on_video, 'right')
        pipe.get_by_name('klv_l').connect('new-sample', self._on_meta,  'left')
        pipe.get_by_name('klv_r').connect('new-sample', self._on_meta,  'right')

        bus = pipe.get_bus()
        bus.add_signal_watch()
        bus.connect('message', self._on_message)
        pipe.set_state(Gst.State.PLAYING)
        self.pipeline = pipe
        print("[PIPELINE] State set to PLAYING")

    def _on_probe(self, pad, info):
        buf = info.get_buffer()
        pts = buf.pts / Gst.SECOND
        print(f"[DEBUG][probe] PTS pré-GPU: {pts:.3f}s")
        if not self.fps_detected:
            caps = pad.get_current_caps() or pad.query_caps()
            self._detect_fps(caps.to_string())
        return Gst.PadProbeReturn.OK

    def _detect_fps(self, caps_str):
        import re
        m = re.search(r'framerate\(fraction\)(\d+)/(\d+)', caps_str)
        if m:
            fps = int(m.group(1))/int(m.group(2))
            self.detected_fps.append(fps)
            if len(self.detected_fps) >= 3:
                from collections import Counter
                fps0 = Counter(self.detected_fps).most_common(1)[0][0]
                self.pts_precision = 1.0 / fps0
                self.fps_detected = True
                print(f"[DEBUG] Final FPS precision: {self.pts_precision:.3f}s (fps={fps0})")

    def _on_meta(self, sink, side):
        sample = sink.emit('pull-sample')
        buf = sample.get_buffer()
        pts = buf.pts / Gst.SECOND
        print(f"[META] side={side}, raw PTS={pts:.3f}s")
        red = self._align_pts(pts)
        if red < SKIP:
            return Gst.FlowReturn.OK
        if self.start_pts is None and buf.pts != Gst.CLOCK_TIME_NONE:
            self.start_pts = pts
            print(f"[META] start_pts initialized to {self.start_pts:.3f}s")
        ok, info = buf.map(Gst.MapFlags.READ)
        fname = None
        if ok:
            data = info.data
            buf.unmap(info)
            if len(data) >= 20:
                length = struct.unpack('>I', data[16:20])[0]
                payload = data[20:20+length]
                msg = info_pb2.StreamInfo()
                try:
                    msg.ParseFromString(payload)
                    fname = msg.filename
                except:
                    pass
        print(f"[META] side={side}, aligned PTS={red:.3f}s, filename={fname}")
        if fname:
            self.sample_queue.put(('klv', side, fname, pts, red))
        return Gst.FlowReturn.OK

    def _on_video(self, sink, side):
        sample = sink.emit('pull-sample')
        buf = sample.get_buffer()
        pts = buf.pts / Gst.SECOND
        print(f"[VIDEO] side={side}, raw PTS={pts:.3f}s")
        if pts <= SKIP:
            return Gst.FlowReturn.OK
        red = self._align_pts(pts)
        if self.start_pts is None:
            self.start_pts = pts
            print(f"[VIDEO] start_pts initialized to {self.start_pts:.3f}s")
        ok, info = buf.map(Gst.MapFlags.READ)
        frame_img = None
        if ok:
            data = info.data
            buf.unmap(info)
            np_arr = np.frombuffer(data, dtype=np.uint8)
            frame_img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        print(f"[VIDEO] side={side}, aligned PTS={red:.3f}s")
        self.sample_queue.put(('frame', side, frame_img, pts, red))
        return Gst.FlowReturn.OK

    def _align_pts(self, pts):
        if pts < 0:
            return -1
        if self.global_offset is None:
            down = math.floor(pts / self.pts_precision) * self.pts_precision
            self.global_offset = pts - down
            print(f"[ALIGN] Initial offset={self.global_offset:.3f}s")
        aligned = math.floor((pts - self.global_offset) / self.pts_precision) * self.pts_precision
        return aligned

    def _process_samples(self):
        print("[PROCESS] Sample processing thread started")
        tol = self.pts_precision / 2
        while self.running:
            try:
                typ, side, data, pts, _ = self.sample_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            with self.lock:
                if typ == 'frame':
                    self.frames[side].append((pts, data))
                else:
                    self.klvs[side].append((pts, data))
                if all(self.frames[s] and self.klvs[s] for s in ('left','right')):
                    f_l_pts, f_l = self.frames['left'][0]
                    f_r_pts, f_r = self.frames['right'][0]
                    k_l_pts, k_l = self.klvs['left'][0]
                    k_r_pts, k_r = self.klvs['right'][0]
                    dt = max(abs(f_l_pts-k_l_pts), abs(f_r_pts-k_r_pts))
                    if dt <= tol:
                        self.frames['left'].popleft()
                        self.frames['right'].popleft()
                        self.klvs['left'].popleft()
                        self.klvs['right'].popleft()
                        print(f"[SYNC] f_pts=({f_l_pts:.3f},{f_r_pts:.3f}) k_pts=({k_l_pts:.3f},{k_r_pts:.3f}) dt={dt:.3f}s")
                        img = np.hstack((f_l, f_r))
                        text = f"LEFT KLV: {os.path.basename(k_l)} | RIGHT KLV: {os.path.basename(k_r)}"
                        cv2.putText(img, text, (10,30), cv2.FONT_HERSHEY_SIMPLEX, 1, (0,255,0),2)
                        name = f"{k_l_pts:.3f}.png".replace('/','_')
                        cv2.imwrite(os.path.join('save', name), img)
                        GLib.idle_add(lambda: (cv2.imshow('Sync', img), cv2.waitKey(1)))
                    else:
                        oldest = min(
                            ('f_l', f_l_pts), ('f_r', f_r_pts), ('k_l', k_l_pts), ('k_r', k_r_pts),
                            key=lambda x: x[1]
                        )[0]
                        if oldest == 'f_l': self.frames['left'].popleft()
                        elif oldest == 'f_r': self.frames['right'].popleft()
                        elif oldest == 'k_l': self.klvs['left'].popleft()
                        else: self.klvs['right'].popleft()

    def _on_message(self, bus, msg):
        if msg.type == Gst.MessageType.ERROR:
            err, dbg = msg.parse_error()
            print("[ERROR]", err.message)
            self.running = False
            self.loop.quit()
        elif msg.type == Gst.MessageType.EOS:
            print("[EOS] End of stream")
            self.running = False
            self.loop.quit()

    def run(self):
        print("[RUN] Starting main loop")
        try:
            self.loop.run()
        except KeyboardInterrupt:
            print("[RUN] Interrupted by user")
        finally:
            self.pipeline.set_state(Gst.State.NULL)
            print("[RUN] Pipeline stopped")

if __name__ == '__main__':
    app = SRTSyncClient()
    app.run()
