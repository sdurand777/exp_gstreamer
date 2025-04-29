
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

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

# --- Configuration ---
IMAGE_DIR_RIGHT = "/home/ivm/escargot/imgs_right_numbered"
IMAGE_DIR_LEFT  = "/home/ivm/escargot/imgs_left_numbered"
PATTERN         = "img%05d.jpg"
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
    def __init__(self, fps=FPS):
        print("[INIT] Initializing SRTSyncClient")
        self.loop = GLib.MainLoop()
        self.sample_queue = queue.Queue()
        self.sync_buffer = defaultdict(lambda: {'f_l':None,'k_l':None,'f_r':None,'k_r':None,
                                                'pts_f_l':None,'pts_k_l':None,'pts_f_r':None,'pts_k_r':None})
        self.global_offset = None
        self.pts_precision = 1.0 / fps
        self.fps_detected = False
        self.lock = threading.Lock()
        self.running = True
        self.start_pts = None

        threading.Thread(target=self._process_samples, daemon=True).start()
        self._build_pipeline()

    def _build_pipeline(self):
        desc = (
            f"srtsrc latency=1000 uri={VIDEO_SRT_URI_LEFT} ! queue !"
            "application/x-rtp,media=video,encoding-name=JPEG,payload=26 ! rtpjpegdepay name=depay_l ! nvjpegdec ! tee name=tee_l "
            "tee_l. ! queue ! videoconvert ! video/x-raw,format=RGB ! appsink name=vid_l emit-signals=true sync=true "
            "tee_l. ! fakesink sync=false "
            f"srtsrc latency=1000 uri={VIDEO_SRT_URI_RIGHT} ! queue !"
            "application/x-rtp,media=video,encoding-name=JPEG,payload=26 ! rtpjpegdepay name=depay_r ! nvjpegdec ! tee name=tee_r "
            "tee_r. ! queue ! videoconvert ! video/x-raw,format=RGB ! appsink name=vid_r emit-signals=true sync=true "
            "tee_r. ! fakesink sync=false "
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
        return Gst.PadProbeReturn.OK

    def _on_meta(self, sink, side):
        sample = sink.emit('pull-sample')
        buf = sample.get_buffer()
        pts = buf.pts / Gst.SECOND
        print(f"[META] side={side}, raw PTS={pts:.3f}s")
        if pts <= SKIP:
            return Gst.FlowReturn.OK
        if self.start_pts is None:
            self.start_pts = pts
            print(f"[META] start_pts initialized to {self.start_pts:.3f}s")
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
        aligned = self._align_pts(pts)
        print(f"[META] side={side}, aligned PTS={aligned:.3f}s, filename={fname}")
        if fname:
            self.sample_queue.put(('klv', side, fname, pts, aligned))
        return Gst.FlowReturn.OK

    def _on_video(self, sink, side):
        sample = sink.emit('pull-sample')
        buf = sample.get_buffer()
        pts = buf.pts / Gst.SECOND
        print(f"[VIDEO] side={side}, raw PTS={pts:.3f}s")
        if pts <= SKIP:
            return Gst.FlowReturn.OK
        # on ne réinitialise plus start_pts ici
        ok, info = buf.map(Gst.MapFlags.READ)
        frame_img = None
        if ok:
            caps = sample.get_caps()
            structure = caps.get_structure(0)
            width = structure.get_value('width')
            height = structure.get_value('height')
            frame_rgb = np.ndarray((height, width, 3), dtype=np.uint8, buffer=info.data)
            buf.unmap(info)
            frame_img = cv2.cvtColor(frame_rgb, cv2.COLOR_RGB2BGR)
        aligned = self._align_pts(pts)
        print(f"[VIDEO] side={side}, aligned PTS={aligned:.3f}s")
        self.sample_queue.put(('frame', side, frame_img, pts, aligned))
        return Gst.FlowReturn.OK

    def _align_pts(self, pts):
        if pts < 0:
            return -1
        if self.global_offset is None:
            down = int(pts / self.pts_precision) * self.pts_precision
            self.global_offset = pts - down
            print(f"[ALIGN] Initial offset={self.global_offset:.3f}s")
        # plus de mises à jour dynamiques
        aligned = round((pts - self.global_offset) / self.pts_precision) * self.pts_precision
        return aligned

    def _process_samples(self):
        print("[PROCESS] Sample processing thread started")
        while self.running:
            try:
                typ, side, data, pts, aligned = self.sample_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            with self.lock:
                ent = self.sync_buffer[aligned]

                # affichage avant insertion
                debug_parts = []
                for name in ("f_l","f_r","k_l","k_r"):
                    pkey = f"pts_{name}"
                    if ent[name] is not None and ent.get(pkey) is not None:
                        debug_parts.append(f"{name}=True({ent[pkey]:.3f}s)")
                    else:
                        debug_parts.append(f"{name}=False")
                print(f"[DEBUG] avant insert red={aligned:.3f}s → {', '.join(debug_parts)}")

                # insert
                key = f"{typ[0]}_{side[0]}"
                ent[key] = data
                ent[f"pts_{typ[0]}_{side[0]}"] = pts

                # affichage après insertion
                debug_parts = []
                for name in ("f_l","f_r","k_l","k_r"):
                    pkey = f"pts_{name}"
                    if ent[name] is not None and ent.get(pkey) is not None:
                        debug_parts.append(f"{name}=True({ent[pkey]:.3f}s)")
                    else:
                        debug_parts.append(f"{name}=False")
                print(f"[DEBUG] après insert red={aligned:.3f}s → {', '.join(debug_parts)}")

                # quand tout est prêt
                if all(ent.get(k) is not None for k in ('f_l','f_r','k_l','k_r')):
                    print(f"[SYNC] red={aligned:.3f}s  fl={ent['pts_f_l']:.3f}s  fr={ent['pts_f_r']:.3f}s  kl={ent['pts_k_l']:.3f}s  kr={ent['pts_k_r']:.3f}s")
                    L, R = ent['f_l'], ent['f_r']
                    img = np.hstack((L,R))
                    ln = ent['k_l'].split('/')[-1] if isinstance(ent['k_l'],str) else ''
                    rn = ent['k_r'].split('/')[-1] if isinstance(ent['k_r'],str) else ''
                    text = f"LEFT: {ln}   |   RIGHT: {rn}"
                    cv2.putText(img, text, (10,30), cv2.FONT_HERSHEY_SIMPLEX,1,(0,255,0),2,cv2.LINE_AA)
                    os.makedirs('save', exist_ok=True)
                    cv2.imwrite(os.path.join('save', f"{aligned:.3f}.png".replace('/','_')), img)
                    GLib.idle_add(lambda: (cv2.imshow('Sync', img), cv2.waitKey(1)))
                    del self.sync_buffer[aligned]

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
