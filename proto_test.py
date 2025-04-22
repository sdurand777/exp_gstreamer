
from info_pb2 import StreamInfo
from google.protobuf.timestamp_pb2 import Timestamp

# Exemple d’instanciation
info = StreamInfo(
    device_id="cam01",
    id=42,
    trig_id=7,
    channel="A",
    clock_id="clk0",
    filename="img0001.jpg",
    session_name="essai"
)
# Remplir un Timestamp
now = Timestamp()
now.GetCurrentTime()
info.timestamp.CopyFrom(now)

# Sérialiser
data = info.SerializeToString()

# Désérialiser
info2 = StreamInfo()
info2.ParseFromString(data)
print(info2)
