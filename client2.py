
#!/usr/bin/env python3
import sys
import struct
import gi
import cv2
import numpy as np
import re
import collections


# votre protobuf
import info_pb2

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

TCP_HOST = "127.0.0.1"
TCP_PORT = 7000

# --- Fonction d'affichage en idle
# Création de la fenêtre une fois
cv2.namedWindow("frame", cv2.WINDOW_AUTOSIZE)

def display_frame(frame):
    """Affiche une frame dans la fenêtre OpenCV."""
    cv2.imshow("frame", frame)
    # waitKey minimal pour rafraîchissement sans blocage lourd
    cv2.waitKey(1)
    # Retour False pour retirer la source après exécution unique
    return False


def parse_klv(buffer):
    """Extrait Key+Length+Value et parse Value en StreamInfo."""
    ok, info = buffer.map(Gst.MapFlags.READ)
    if not ok:
        return None, "Erreur de mapping"
    data = info.data
    buffer.unmap(info)

    if len(data) < 20:
        return None, f"Buffer trop petit ({len(data)} bytes)"

    # On saute les 16 octets de Key
    length = struct.unpack(">I", data[16:20])[0]
    if len(data) < 20 + length:
        return None, f"Payload tronquée : length={length}, dispo={len(data)-20}"

    payload = data[20:20+length]
    msg = info_pb2.StreamInfo()
    try:
        msg.ParseFromString(payload)
        return msg, None
    except Exception as e:
        return None, str(e)


def on_new_klv_sample(sink):
    print("on new klv sample")
    sample = sink.emit("pull-sample")
    buf = sample.get_buffer()
    # timestamp
    pts = buf.pts
    secs = pts / Gst.SECOND if pts != Gst.CLOCK_TIME_NONE else -1


    # print timestamp
    pts_time = pts / Gst.SECOND if pts != Gst.CLOCK_TIME_NONE else -1


    print("####### pts : ", pts_time)


    msg, err = parse_klv(buf)
    if msg:
        print(f"[{msg.filename}] systemtime={msg.systemtime.seconds}.{msg.systemtime.nanos:09d} session_name{msg.session_name}")
    else:
        print(f"KLV @ {secs:.3f}s → Erreur : {err}")
    return Gst.FlowReturn.OK



detected_fps_values = []
pts_precision = None
fps_detected = False

def detect_framerate_from_caps(caps_str):
    global detected_fps_values, pts_precision, fps_detected
    """Extract framerate from caps string and calculate precision."""
    # Try to find framerate in caps string
    # print("--------------------")
    # print(caps_str)
    # print("--------------------")
    fps_match = re.search(r'framerate=\(fraction\)(\d+)/(\d+)', caps_str)
    if fps_match:
        num = int(fps_match.group(1))
        denom = int(fps_match.group(2))
        if denom > 0:
            fps = num / denom
            # Store detected fps
            detected_fps_values.append(fps)
            
            # Only set precision once we have enough samples
            if len(detected_fps_values) >= 3:
                # Use the most common fps value
                fps_counter = collections.Counter(detected_fps_values)
                most_common_fps = fps_counter.most_common(1)[0][0]
                
                # Set precision to 1/framerate (frame duration)
                if most_common_fps > 0:
                    pts_precision = 1.0 / most_common_fps
                else:
                    pts_precision = 0.25
                # print(f"Detected framerate: {most_common_fps} fps")
                # print(f"Setting PTS precision to: {pts_precision:.6f}s")
                fps_detected = True
    
    # Default precision if no framerate found
    if pts_precision is None:
        pts_precision = 0.25  # Default value






# Callback pour traiter chaque nouvelle sample
def on_new_video_sample(appsink):
    print("on new video sample")
    sample = appsink.emit('pull-sample')

    buf = sample.get_buffer()

    # print timestamp
    pts = buf.pts
    pts_time = pts / Gst.SECOND if pts != Gst.CLOCK_TIME_NONE else -1

    print("========== pts : ", pts_time)

    # print image name

    result, mapinfo = buf.map(Gst.MapFlags.READ)
    if not result:
        return Gst.FlowReturn.OK

    # Obtenir la taille de l'image
    caps = sample.get_caps()

    # detection fps 
    caps_str = caps.to_string()
    detect_framerate_from_caps(caps_str)
    

    structure = caps.get_structure(0)
    width = structure.get_value('width')
    height = structure.get_value('height')
    pixel_format = structure.get_value('format')
    print(f"processing image: {width}x{height}, format: {pixel_format}")

    # # traiter selon le format de pixel
    # if pixel_format == 'RGB':
    #     channels = 3
    #     dtype = np.uint8
    #     frame_data = np.ndarray((height, width, channels), dtype=dtype, buffer=bytes(mapinfo.data))
    #     frame_rgb = cv2.cvtColor(frame_data, cv2.COLOR_RGB2BGR)
    #     cv2.imshow("frame", frame_rgb)
    #     cv2.waitKey(0)
    #
    # buf.unmap(mapinfo)

    # Conversion en NumPy selon le format
    if pixel_format == 'RGB':
        channels = 3
        dtype = np.uint8
        frame_data = np.ndarray((height, width, channels), dtype=dtype, buffer=bytes(mapinfo.data))
        frame_bgr = cv2.cvtColor(frame_data, cv2.COLOR_RGB2BGR)
    else:
        # ajouter d'autres formats si besoin
        buf.unmap(mapinfo)
        return Gst.FlowReturn.OK

    buf.unmap(mapinfo)

    # Planifier l'affichage dans la boucle GLib
    GLib.idle_add(display_frame, frame_bgr)

    return Gst.FlowReturn.OK



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


# --- main application
def main():
    Gst.init(None)

    meta_pipeline = Gst.parse_launch(
        f"tcpclientsrc host={TCP_HOST} port={TCP_PORT} ! queue ! tsdemux name=dmx "
        #"dmx. ! queue ! meta/x-klv ! queue ! appsink name=klv_sink emit-signals=true drop=true"
        "dmx. ! queue ! meta/x-klv ! queue ! appsink name=klv_sink emit-signals=true sync=true"
    )
    sink = meta_pipeline.get_by_name("klv_sink")
    sink.connect("new-sample", on_new_klv_sample)

        # URI et pipeline
    SRC_URI = "srt://127.0.0.1:6020?mode=caller"

    # # Créer le pipeline via Gst.parse_launch (syntax inspirée de l'exemple)
    # video_pipeline = Gst.parse_launch(
    #     f"srtsrc uri={SRC_URI} ! "
    #     "application/x-rtp,encoding-name=JPEG ! "
    #     "rtpjpegdepay ! jpegdec ! videoconvert ! autovideosink sync=true"
    # )

    # Construire le pipeline avec appsink pour récupérer les frames par signal
    # video_pipeline_description = (
    #     f"srtsrc uri={SRC_URI} ! "
    #     "application/x-rtp,media=video,encoding-name=JPEG,payload=26 ! queue ! rtpjitterbuffer ! rtpjpegdepay ! jpegdec ! "
    #     "videoconvert ! video/x-raw,format=RGB ! "
    #     "appsink name=video_sink emit-signals=true sync=false"
    # )

    # video_pipeline_description = (
    #     f"srtsrc uri={SRC_URI} ! queue ! decodebin ! "
    #     "videoconvert ! video/x-raw,format=RGB ! "
    #     "appsink name=video_sink emit-signals=true sync=false"
    # )

    video_pipeline_description = (
        f"srtsrc latency=1000 uri={SRC_URI} ! "
        "application/x-rtp,media=video,clock-rate=90000,encoding-name=JPEG,payload=26 ! queue ! "
        "rtpjpegdepay ! nvjpegdec ! queue ! "
        "videoconvert ! video/x-raw,format=RGB ! "
        "appsink name=video_sink emit-signals=true sync=true"
    )

    video_pipeline = Gst.parse_launch(video_pipeline_description)

    # get sink to connect callback
    video_sink = video_pipeline.get_by_name("video_sink")
    # Connecter le signal
    video_sink.connect('new-sample', on_new_video_sample)

    loop = GLib.MainLoop()

    for pipeline in (meta_pipeline, video_pipeline):
        bus = pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect('message', on_message, loop)

    meta_pipeline.set_state(Gst.State.PLAYING)
    video_pipeline.set_state(Gst.State.PLAYING)

    print(f"Streaming video (SRT) on {SRC_URI} and KLV-TS (TCP) on tcp://{TCP_HOST}:{TCP_PORT}")


    # ret, state, pending = video_pipeline.get_state(2 * Gst.SECOND)
    # print("video_pipeline state:", state, "pending:", pending)

    try:
        loop.run()
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        meta_pipeline.set_state(Gst.State.NULL)
        video_pipeline.set_state(Gst.State.NULL)

if __name__ == "__main__":
    main()
