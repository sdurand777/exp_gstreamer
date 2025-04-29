
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
from collections import defaultdict
import math

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

# --- Configuration ---

IMAGE_DIR_RIGHT     = "/home/smith/dataset/sequences/00/image_0/jpgs_numbered"
IMAGE_DIR_LEFT      = "/home/smith/dataset/sequences/00/image_1/jpgs_numbered"

PATTERN             = "%05d.jpg"


FPS             = 4
VIDEO_SRT_URI_LEFT  = "srt://127.0.0.1:6020?mode=caller"
VIDEO_SRT_URI_RIGHT = "srt://127.0.0.1:6021?mode=caller"
TCP_HOST            = "127.0.0.1"
TCP_PORT            = 7000
SKIP = 3

# Initialize GStreamer and keys/indexes
Gst.init(None)
_key = hashlib.md5(b"StreamInfo").digest()

class SRTSyncClient:
    def __init__(self, fps=4):
        print("[INIT] Initializing SRTSyncClient")
        self.loop = GLib.MainLoop()
        self.sample_queue = queue.Queue()
        self.sync_buffer = defaultdict(lambda: {'f_l':None,'k_l':None,'f_r':None,'k_r':None,
                                                'pts_f_l':None,'pts_k_l':None,'pts_f_r':None,'pts_k_r':None})
        self.global_offset = None
        self.pts_precision = None
        self.fps_detected = False
        self.detected_fps = []
        self.lock = threading.Lock()
        self.running = True
        self.start_pts = None
        self.pts_precision = 1/FPS

        threading.Thread(target=self._process_samples, daemon=True).start()
        self._build_pipeline()

    def _build_pipeline(self):
        desc = (
            f"srtsrc latency=1000 uri={VIDEO_SRT_URI_LEFT} ! queue !"
            "application/x-rtp,media=video,encoding-name=JPEG,payload=26 ! rtpjpegdepay name=depay_l ! jpegparse ! tee name=tee_l "
            "tee_l. ! appsink name=vid_{side} caps=\"image/jpeg\" name=vid_l emit-signals=true sync=true "
            f"srtsrc latency=1000 uri={VIDEO_SRT_URI_RIGHT} ! queue !"
            "application/x-rtp,media=video,encoding-name=JPEG,payload=26 ! rtpjpegdepay name=depay_r ! jpegparse ! tee name=tee_r "
            "tee_r. ! appsink name=vid_{side} caps=\"image/jpeg\"  name=vid_r emit-signals=true sync=true "
            f"tcpclientsrc host={TCP_HOST} port={TCP_PORT} ! tsdemux name=dmx "
            "dmx. ! queue ! meta/x-klv,parsed=true ! appsink name=klv_l emit-signals=true sync=false drop=false "
            "dmx. ! queue ! meta/x-klv,parsed=true ! appsink name=klv_r emit-signals=true sync=false drop=false"
        )
        print("[PIPELINE] Launching pipeline:\n", desc)
        pipe = Gst.parse_launch(desc)
        # pad-probes to capture PTS before GPU decode
        for elem_name in ('depay_l', 'depay_r'):
            elem = pipe.get_by_name(elem_name)
            pad = elem.get_static_pad('src')
            pad.add_probe(Gst.PadProbeType.BUFFER, self._on_probe)

        # connect sinks
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
            print(f"[DEBUG] Detected FPS sample: {fps}")
            if len(self.detected_fps) >= 3:
                from collections import Counter
                fps0 = Counter(self.detected_fps).most_common(1)[0][0]
                self.pts_precision = 1.0 / fps0
                self.fps_detected = True
                print(f"[DEBUG] Final FPS precision: {self.pts_precision:.3f}s (fps={fps0})")
        if self.pts_precision is None:
            self.pts_precision = 1.0 / FPS

    def _on_meta(self, sink, side):
        sample = sink.emit('pull-sample')
        buf = sample.get_buffer()
        pts = buf.pts / Gst.SECOND
        print(f"[META] side={side}, raw PTS={pts:.3f}s")
        if pts <= SKIP or (self.start_pts and pts < self.start_pts):
            return Gst.FlowReturn.OK
        if self.start_pts is None and buf.pts != Gst.CLOCK_TIME_NONE:
            self.start_pts = pts
            print(f"[META] start_pts initialized to {self.start_pts:.3f}s")
            #return Gst.FlowReturn.OK
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
        red = self._align_pts(pts)
        print(f"[META] side={side}, aligned PTS={red:.3f}s, filename={fname}")
        if fname:
            self.sample_queue.put(('klv', side, fname, pts, red))
        return Gst.FlowReturn.OK


    def _on_meta(self, sink, side):
        sample = sink.emit('pull-sample')
        buf = sample.get_buffer()
        pts = buf.pts / Gst.SECOND
        print(f"[META] side={side}, raw PTS={pts:.3f}s")

        # 1) calcul de l’PTS aligné
        red = self._align_pts(pts)

        # 2) on skippe les méta vraiment trop tôt (sur la base de l’aligned PTS)
        if red < SKIP:
            return Gst.FlowReturn.OK

        # 3) initialisation de start_pts une seule fois, sans return
        if self.start_pts is None and buf.pts != Gst.CLOCK_TIME_NONE:
            self.start_pts = pts
            print(f"[META] start_pts initialized to {self.start_pts:.3f}s")

        # 4) lecture du payload KLV
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

        # 5) affichage et mise en queue
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

        # red = self._align_pts(pts)
        # if red < SKIP:
        #     return Gst.FlowReturn.OK

        if self.start_pts is None:
            self.start_pts = pts
            print(f"[VIDEO] start_pts initialized to {self.start_pts:.3f}s")
        # Decode using caps-based raw buffer mapping
        ok, info = buf.map(Gst.MapFlags.READ)
        frame_img = None
        if ok:
            data = info.data
            buf.unmap(info)
            np_arr = np.frombuffer(data, dtype=np.uint8)
            frame_img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

            # caps = sample.get_caps()
            # structure = caps.get_structure(0)
            # width = structure.get_value('width')
            # height = structure.get_value('height')
            # # assume RGB format with 3 channels
            # frame_rgb = np.ndarray((height, width, 3), dtype=np.uint8, buffer=info.data)
            # buf.unmap(info)
            # frame_img = cv2.cvtColor(frame_rgb, cv2.COLOR_RGB2BGR)

        base_aligned = self._align_pts(pts)
        # check raw KLV stored for this bucket
        ent = self.sync_buffer[base_aligned]
        raw_klv = ent.get(f'pts_klv_{side}')
        if raw_klv is not None and raw_klv > pts:
            aligned = base_aligned - self.pts_precision
            print(f"[VIDEO] side={side}, raw KLV {raw_klv:.3f}s > raw frame {pts:.3f}s, moving to {aligned:.3f}s")
        else:
            aligned = base_aligned
            print(f"[VIDEO] side={side}, aligned={aligned:.3f}s")
        ent = self.sync_buffer[aligned]
        ent[f'f_{side}'] = frame_img
        ent[f'pts_frame_{side}'] = pts
        self.sample_queue.put(('frame', side, frame_img, pts, aligned))
        return Gst.FlowReturn.OK
        # print(f"[VIDEO] side={side}, aligned PTS={red:.3f}s")
        # self.sample_queue.put(('frame', side, frame_img, pts, red))
        # return Gst.FlowReturn.OK





    def _align_pts(self, pts):
        if pts < 0:
            return -1
        if self.global_offset is None:
            down = int(pts / self.pts_precision) * self.pts_precision
            self.global_offset = pts - down
            print(f"[ALIGN] Initial offset={self.global_offset:.3f}s")
        #aligned = round((pts - self.global_offset) / self.pts_precision) * self.pts_precision
        aligned = math.floor((pts - self.global_offset) / self.pts_precision) * self.pts_precision
        return aligned

    def _process_samples(self):
        print("[PROCESS] Sample processing thread started")
        while self.running:
            try:
                typ, side, data, pts, red = self.sample_queue.get(timeout=0.1)
                print(f"[QUEUE] Received {typ} {side}, raw PTS={pts:.3f}s, red={red:.3f}s")
            except queue.Empty:
                continue
            with self.lock:
                ent = self.sync_buffer[red]
                print(f"[DEBUG] Bucket before insert red={red:.3f}s -> f_l={ent['f_l'] is not None}, f_r={ent['f_r'] is not None}, k_l={ent['k_l'] is not None}, k_r={ent['k_r'] is not None}")
                key = f"{typ[0]}_{side[0]}"  # f=frame/k=klv, l=left/r=right
                ent[key] = data
                ent[f"pts_{typ[0]}_{side[0]}"] = pts
                print(f"[DEBUG] Bucket after insert red={red:.3f}s -> f_l={ent['f_l'] is not None}, f_r={ent['f_r'] is not None}, k_l={ent['k_l'] is not None}, k_r={ent['k_r'] is not None}")
                if all(ent.get(k) is not None for k in ('f_l','f_r','k_l','k_r')):
                    print(f"[SYNC] red={red:.3f}s fl={ent['pts_f_l']:.3f}s fr={ent['pts_f_r']:.3f}s kl={ent['pts_k_l']:.3f}s kr={ent['pts_k_r']:.3f}s")
                    L, R = ent['f_l'], ent['f_r']
                    img = np.hstack((L, R))

                    # draw KLV filenames on image
                    left_name = ent['k_l'].split('/')[-1] if isinstance(ent['k_l'], str) else ''
                    right_name = ent['k_r'].split('/')[-1] if isinstance(ent['k_r'], str) else ''
                    text = f"LEFT: {left_name}   |   RIGHT: {right_name}"
                    cv2.putText(img, text, (10, 30), cv2.FONT_HERSHEY_SIMPLEX,
                                1, (0, 255, 0), 2, cv2.LINE_AA)

                    name = f"{red:.3f}.png".replace('/', '_')
                    cv2.imwrite(os.path.join('save', name), img)
                    GLib.idle_add(lambda: (cv2.imshow('Sync', img), cv2.waitKey(1)))
                    del self.sync_buffer[red]

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
