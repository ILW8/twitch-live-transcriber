import datetime
import json
import streamlink
import ffmpeg
from id3parse import ID3, TAG_HEADER_SIZE


# def cut_to_latest_packet_start(data: bytearray) -> bytearray:
#     for i in range(len(data) - 1 - 3, -1, -1):  # -3 because TS packet header is 4 bytes
#         if data[i] == "\x47":
#             asc = data[i + 3] & 0x30
#             if asc == 0x00:
#                 # ASC of 0 is reserved for future use, meaning this is most likely not a TS packet header
#                 continue
#
#             pid = data[i+1:i+3] & 0x1FFF
#             if pid == 0x1FFF:
#             print("found ")
#         print(i)
#     return bytearray()


if __name__ == '__main__':
    streams = streamlink.streams("https://twitch.tv/btmc")
    audio_stream = streams.get("audio_only", None)
    assert audio_stream is not None
    with audio_stream.open() as stream_handle:
        buf = bytearray()
        packet_corrupt = False
        last_read_count = 0
        while True:
            if not packet_corrupt and len(buf) > 0:
                cut_amount = 0
                for byte_index in range(last_read_count, len(buf)):
                    if buf[byte_index] == 0x47:
                        cut_amount = byte_index
                buf = buf[:-cut_amount]
            packet_corrupt = False
            tmp_data = stream_handle.read(16384)
            last_read_count = len(tmp_data)
            buf += tmp_data
            a = ffmpeg.input("pipe:", f="mpegts")
            b = a['d:0'].output("pipe:", f="data")
            ffmpeg_id3_extract = b.run_async(pipe_stdin=True, pipe_stdout=True, quiet=True)
            stdout, stderr = ffmpeg_id3_extract.communicate(input=buf)

            stderr = stderr.decode('utf-8')
            if "Packet corrupt" in stderr or 'matches no streams' in stderr:
                packet_corrupt = True
                continue

            # decoded = ''.join(map(lambda ch: ch if ch in set(string.printable) else ".", stdout.decode("ascii")))
            # print(decoded)
            while len(stdout) > 0:
                id3data = ID3.from_byte_array(stdout)
                twitch_txxx_segment_meta = json.loads(id3data.find_frame_by_name("TXXX").raw_bytes[17:].decode('utf-8'))
                print(f"[{datetime.datetime.now(tz=datetime.timezone.utc).isoformat()}] "
                      f"{twitch_txxx_segment_meta}")
                stdout = stdout[id3data.header.tag_size + TAG_HEADER_SIZE:]
