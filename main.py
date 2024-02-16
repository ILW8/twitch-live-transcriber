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


"""
https://arca.live/b/twitchdev/48875066
https://web.archive.org/save/https://arca-live.translate.goog/b/twitchdev/48875066?_x_tr_sl=auto&_x_tr_tl=en&_x_tr_hl=en&_x_tr_pto=wapp
"""
if __name__ == '__main__':
    streams = streamlink.streams("https://twitch.tv/btmc")
    audio_stream = streams.get("audio_only", None)
    assert audio_stream is not None

    with audio_stream.open() as stream_handle:
        buf = bytearray()
        buffer_incomplete = False  # incomplete when not enough data is present to extract ID3 entry
        last_read_count = 0
        while True:
            if not buffer_incomplete and len(buf) > 0:
                cut_amount = 0
                for byte_index in range(last_read_count, len(buf)):
                    if buf[byte_index] == 0x47:
                        cut_amount = byte_index
                buf = buf[:-cut_amount]
            buffer_incomplete = False
            tmp_data = stream_handle.read(16384)
            last_read_count = len(tmp_data)
            buf += tmp_data
            ffmpeg_id3_extract = (ffmpeg
                                  .input("pipe:", f="mpegts")['d:0']
                                  .output("pipe:", f="data")
                                  .run_async(pipe_stdin=True, pipe_stdout=True, quiet=True))
            stdout, stderr = ffmpeg_id3_extract.communicate(input=buf)

            if "Packet corrupt" in (stderr := stderr.decode('utf-8')) or 'matches no streams' in stderr:
                buffer_incomplete = True
                continue

            # decoded = ''.join(map(lambda ch: ch if ch in set(string.printable) else ".", stdout.decode("ascii")))
            # print(decoded)
            while len(stdout) > 0:
                id3data = ID3.from_byte_array(stdout)
                twitch_txxx_segment_meta = json.loads(id3data.find_frame_by_name("TXXX").raw_bytes[17:].decode('utf-8'))
                print(f"[{datetime.datetime.now(tz=datetime.timezone.utc).isoformat()}] "
                      f"stream timestamp: {datetime.timedelta(seconds=twitch_txxx_segment_meta['stream_offset'])}")
                stdout = stdout[id3data.header.tag_size + TAG_HEADER_SIZE:]
