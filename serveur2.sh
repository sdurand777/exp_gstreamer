# Dossier contenant les JPEG numérotés séquentiellement à partir de 1
IMAGE_DIR="/home/smith/dataset/sequences/00/image_0/jpgs"
PATTERN="%06d.jpg"   # img0001.jpg, img0002.jpg, …

# Fréquence d’affichage (ici 2 images par seconde)
FPS=10

cmd="""
-m multifilesrc location=\"${IMAGE_DIR}/${PATTERN}\" index=1 caps=\"image/jpeg,framerate=${FPS}/1\" \
! decodebin \
! videoconvert \
! video/x-raw,format=I420 \
! jpegenc \
! jpegparse \
! rtpjpegpay mtu=1316 \
! srtserversink uri=\"srt://127.0.0.1:6020?mode=listener\"
"""
gst-launch-1.0 $cmd
