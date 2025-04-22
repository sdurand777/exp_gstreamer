
#!/usr/bin/env python3
import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GObject, GLib

# --- Configuration ---
IMAGE_DIR = "/home/smith/dataset/sequences/00/image_0/jpgs"
PATTERN   = "%06d.jpg"   # ex. 000001.jpg, 000002.jpg, …
FPS       = 10
SINK_URI  = "srt://127.0.0.1:6020?mode=listener"

# --- Gestion des messages du bus GStreamer ---
def on_message(bus, message, loop):
    t = message.type
    if t == Gst.MessageType.ERROR:
        err, dbg = message.parse_error()
        print(f"[ERREUR] {err.message}")
        if dbg:
            print("Debug :", dbg)
        loop.quit()
    elif t == Gst.MessageType.EOS:
        print("[FIN] End-Of-Stream")
        loop.quit()

def main():
    # Initialisations
    GObject.threads_init()
    Gst.init(None)

    # Construction du pipeline en une seule chaîne
    pipeline_description = (
        f"multifilesrc location={IMAGE_DIR}/{PATTERN} index=1 "
        f"caps=\"image/jpeg,framerate={FPS}/1\" "
        "! decodebin "
        "! videoconvert "
        "! video/x-raw,format=I420 "
        "! jpegenc "
        "! jpegparse "
        "! rtpjpegpay mtu=1316 "
        f"! srtserversink uri={SINK_URI}"
    )
    pipeline = Gst.parse_launch(pipeline_description)

    # Bus pour recevoir erreurs et EOS
    bus = pipeline.get_bus()
    loop = GObject.MainLoop()
    bus.add_signal_watch()
    bus.connect("message", on_message, loop)

    # Démarrage
    pipeline.set_state(Gst.State.PLAYING)
    print("Pipeline en cours d'exécution… Ctrl+C pour interrompre.")
    try:
        loop.run()
    except KeyboardInterrupt:
        print("\nInterruption par l’utilisateur")
    finally:
        pipeline.set_state(Gst.State.NULL)

if __name__ == "__main__":
    main()
