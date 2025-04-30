
#!/usr/bin/env python3
import os
import gi
import info_pb2
from google.protobuf.timestamp_pb2 import Timestamp
import hashlib
import struct
import threading
import queue
import numpy as np
import cv2
import math
from collections import defaultdict

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

# --- Configuration ---
FPS = 4  # images per second
VIDEO_SRT_URI_LEFT  = "srt://127.0.0.1:6020?mode=caller"
VIDEO_SRT_URI_RIGHT = "srt://127.0.0.1:6021?mode=caller"
TCP_HOST = "127.0.0.1"
TCP_PORT = 7000
SKIP = 3  # skip initial seconds

# Initialize GStreamer
Gst.init(None)
_key = hashlib.md5(b"StreamInfo").digest()

class SRTSyncClient:
    def __init__(self, fps=FPS):
        print("[INIT] Initializing SRTSyncClient")
        self.loop = GLib.MainLoop()
        self.queue = queue.Queue()
        self.buffer = defaultdict(dict)
        self.lock = threading.Lock()
        self.running = True
        self.pts_precision = 1.0 / fps
        self.global_offset = None

        threading.Thread(target=self._process, daemon=True).start()
        self._build_pipeline()

    def _build_pipeline(self):
        desc = (
            f"srtsrc latency=1000 uri={VIDEO_SRT_URI_LEFT} ! queue !"
            f"application/x-rtp,media=video,encoding-name=JPEG,payload=26,framerate={FPS}/1 ! rtpjpegdepay name=dep_l ! nvjpegdec ! tee name=tee_l "
            "tee_l. ! queue ! videoconvert ! video/x-raw,format=RGB ! appsink name=vid_l emit-signals=true sync=true "
            "tee_l. ! fakesink sync=false "
            f"srtsrc latency=1000 uri={VIDEO_SRT_URI_RIGHT} ! queue !"
            f"application/x-rtp,media=video,encoding-name=JPEG,payload=26,framerate={FPS}/1 ! rtpjpegdepay name=dep_r ! nvjpegdec ! tee name=tee_r "
            "tee_r. ! queue ! videoconvert ! video/x-raw,format=RGB ! appsink name=vid_r emit-signals=true sync=true "
            "tee_r. ! fakesink sync=false "
            f"tcpclientsrc host={TCP_HOST} port={TCP_PORT} ! tsdemux name=dmx "
            f"dmx. ! queue ! meta/x-klv,parsed=true,framerate={FPS}/1 ! appsink name=klv_l emit-signals=true sync=false drop=false "
            f"dmx. ! queue ! meta/x-klv,parsed=true,framerate={FPS}/1 ! appsink name=klv_r emit-signals=true sync=false drop=false"
        )
        print("[PIPELINE] Launching pipeline:\n", desc)
        pipe = Gst.parse_launch(desc)
        for elem_name in ('dep_l', 'dep_r'):
            elem = pipe.get_by_name(elem_name)
            pad = elem.get_static_pad('src')
            pad.add_probe(Gst.PadProbeType.BUFFER, self._on_probe)

        pipe.get_by_name('vid_l').connect('new-sample', self._on_video, 'left')
        pipe.get_by_name('vid_r').connect('new-sample', self._on_video, 'right')
        pipe.get_by_name('klv_l').connect('new-sample', self._on_meta, 'left')
        pipe.get_by_name('klv_r').connect('new-sample', self._on_meta, 'right')
        pipe.set_state(Gst.State.PLAYING)
        self.pipeline = pipe
        print("[PIPELINE] State set to PLAYING")

    def _on_probe(self, pad, info):
        buf = info.get_buffer()
        pts = buf.pts / Gst.SECOND
        print(f"[DEBUG][probe] PTS pré-GPU: {pts:.3f}s")
        return Gst.PadProbeReturn.OK

    def _align(self, pts):
        if self.global_offset is None:
            base = int(pts / self.pts_precision) * self.pts_precision
            self.global_offset = pts - base
            print(f"[ALIGN] Initial offset={self.global_offset:.3f}s")
        aligned = round((pts - self.global_offset) / self.pts_precision) * self.pts_precision
        print(f"[ALIGN] pts={pts:.3f}s -> aligned={aligned:.3f}s")
        return aligned

    def _on_meta(self, sink, side):
        sample = sink.emit('pull-sample')
        buf = sample.get_buffer()
        pts = buf.pts / Gst.SECOND
        print(f"[META] side={side}, raw PTS={pts:.3f}s")
        if pts <= SKIP:
            return Gst.FlowReturn.OK
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
        aligned = self._align(pts)
        print(f"[META] side={side}, aligned PTS={aligned:.3f}s, filename={fname}")
        # store raw KLV in buffer for decision
        ent = self.buffer[aligned]
        ent[f'pts_klv_{side}'] = pts
        ent[f'k_{side}'] = fname
        self.queue.put(('klv', side, fname, pts, aligned))
        return Gst.FlowReturn.OK

    def _on_video(self, sink, side):
        sample = sink.emit('pull-sample')
        buf = sample.get_buffer()
        pts = buf.pts / Gst.SECOND
        print(f"[VIDEO] side={side}, raw PTS={pts:.3f}s")
        if pts <= SKIP:
            return Gst.FlowReturn.OK
        ok, info = buf.map(Gst.MapFlags.READ)
        frame = None
        if ok:
            caps = sample.get_caps().get_structure(0)
            w, h = caps.get_value('width'), caps.get_value('height')
            frame = cv2.cvtColor(np.ndarray((h, w, 3), np.uint8, info.data), cv2.COLOR_RGB2BGR)
            buf.unmap(info)
        base_aligned = self._align(pts)
        # check raw KLV stored for this bucket
        ent = self.buffer[base_aligned]
        raw_klv = ent.get(f'pts_klv_{side}')
        if raw_klv is not None and raw_klv > pts:
            aligned = base_aligned - self.pts_precision
            print(f"[VIDEO] side={side}, raw KLV {raw_klv:.3f}s > raw frame {pts:.3f}s, moving to {aligned:.3f}s")
        else:
            aligned = base_aligned
            print(f"[VIDEO] side={side}, aligned={aligned:.3f}s")
        ent = self.buffer[aligned]
        ent[f'f_{side}'] = frame
        ent[f'pts_frame_{side}'] = pts
        self.queue.put(('frame', side, frame, pts, aligned))
        return Gst.FlowReturn.OK

    def _process(self):
        print("[PROCESS] Sample processing thread started")
        while self.running:
            try:
                typ, side, data, pts, aligned = self.queue.get(timeout=0.1)
            except queue.Empty:
                continue
            with self.lock:
                ent = self.buffer[aligned]
                # debug before insert
                dbg = []
                for name in ('f_left','f_right','k_left','k_right'):
                    key_raw = 'pts_klv_' + name.split('_')[1] if name.startswith('k_') else 'pts_frame_' + name.split('_')[1]
                    if ent.get(key_raw) is not None:
                        dbg.append(f"{name}=True({ent[key_raw]:.3f}s)")
                    else:
                        dbg.append(f"{name}=False")
                print(f"[DEBUG] avant insert red={aligned:.3f}s -> {', '.join(dbg)}")

                # data already inserted in on_meta/on_video
                # debug after insert
                dbg = []
                for name in ('f_left','f_right','k_left','k_right'):
                    key_raw = 'pts_klv_' + name.split('_')[1] if name.startswith('k_') else 'pts_frame_' + name.split('_')[1]
                    if ent.get(key_raw) is not None:
                        dbg.append(f"{name}=True({ent[key_raw]:.3f}s)")
                    else:
                        dbg.append(f"{name}=False")
                print(f"[DEBUG] après insert red={aligned:.3f}s -> {', '.join(dbg)}")

                # sync
                keys = ['f_left','f_right','k_left','k_right']
                if all(ent.get(n) is not None for n in keys):
                    print(f"[SYNC] red={aligned:.3f}s  fl={ent['pts_frame_left']:.3f}s  fr={ent['pts_frame_right']:.3f}s  kl={ent['pts_klv_left']:.3f}s  kr={ent['pts_klv_right']:.3f}s")
                    L, R = ent['f_left'], ent['f_right']
                    img = np.hstack((L, R))
                    ln = os.path.basename(ent['k_left'])
                    rn = os.path.basename(ent['k_right'])
                    text = f"LEFT: {ln} | RIGHT: {rn}"
                    cv2.putText(img, text, (10,30), cv2.FONT_HERSHEY_SIMPLEX,1,(0,255,0),2)
                    os.makedirs('save', exist_ok=True)
                    cv2.imwrite(f"save/{aligned:.3f}.png", img)
                    GLib.idle_add(lambda i=img: (cv2.imshow('Sync', i), cv2.waitKey(1)))
                    del self.buffer[aligned]

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
    SRTSyncClient().run()
