import datetime
import json
from json import JSONDecodeError

import streamlink
import ffmpeg
from id3parse import ID3, TAG_HEADER_SIZE


TS_PACKET_LENGTH = 188
TS_SYNC_BYTE = 0x47


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


class SegmentWithMeta:
    def __init__(self, segment):
        self.segment = segment

        ffmpeg_id3_extract = (ffmpeg
                              .input("pipe:", f="mpegts")['d:0']
                              .output("pipe:", f="data")
                              .run_async(pipe_stdin=True, pipe_stdout=True, quiet=True))
        stdout, stderr = ffmpeg_id3_extract.communicate(input=segment)

        if "Packet corrupt" in (stderr := stderr.decode('utf-8')) or 'matches no streams' in stderr:
            raise ValueError("unable to parse segment or something")

        twitch_txxx_segment_meta = None
        while len(stdout) > 0:
            id3data = ID3.from_byte_array(stdout)
            try:
                twitch_txxx_segment_meta = json.loads(
                    id3data.find_frame_by_name("TXXX").raw_bytes[17:].decode('utf-8')
                )
                print(f"[{datetime.datetime.now(tz=datetime.timezone.utc).isoformat()}] "
                      f"size: {len(segment)} "
                      f"stream timestamp: {datetime.timedelta(seconds=twitch_txxx_segment_meta['stream_offset'])}")
            except JSONDecodeError:
                pass
            stdout = stdout[id3data.header.tag_size + TAG_HEADER_SIZE:]
        if twitch_txxx_segment_meta is None:
            raise ValueError("couldn't parse segment metadata")
        self.txxx_meta = twitch_txxx_segment_meta

    def __repr__(self):
        return (f"<{self.__class__.__name__} "
                f"bytes={len(self.segment)} "
                f"meta_timestamp={datetime.timedelta(seconds=self.txxx_meta['stream_offset'])}>")


class CircularSegmentBuffer:
    def __init__(self, max_segment_count=5):
        self.segments: [SegmentWithMeta] = []
        self.max_segment_count = max_segment_count

    def append(self, segment: bytes):
        if len(self.segments) >= self.max_segment_count:
            self.pop()
        self.segments.append(SegmentWithMeta(segment))

    def pop(self):
        return self.segments.pop(0)


def ts_get_pid(packet: bytes) -> int:
    assert len(packet) == TS_PACKET_LENGTH
    return ((packet[1] & 0x1F) << 8) | (packet[2] & 0xFF)


"""
https://arca.live/b/twitchdev/48875066
https://web.archive.org/save/https://arca-live.translate.goog/b/twitchdev/48875066?_x_tr_sl=auto&_x_tr_tl=en&_x_tr_hl=en&_x_tr_pto=wapp
"""
if __name__ == '__main__':
    streams = streamlink.streams("https://twitch.tv/rainbow6")
    audio_stream = streams.get("audio_only", None)
    assert audio_stream is not None

    with audio_stream.open() as stream_handle:
        segments_buffer = CircularSegmentBuffer()
        segment_buffer = bytearray()
        segment_incomplete = False  # incomplete when not enough data is present to extract ID3 entry
        last_read_count = 0
        while True:
            if not segment_incomplete and len(segment_buffer) > 0:
                # cut_amount = 0
                # for byte_index in range(last_read_count, len(segment_buffer)):
                #     if segment_buffer[byte_index] == 0x47:
                #         cut_amount = byte_index
                # segment_buffer = segment_buffer[:-cut_amount]
                pass
            segment_incomplete = False
            ts_packet = stream_handle.read(TS_PACKET_LENGTH)
            if not ts_packet:
                break

            # resync
            while ts_packet[0] != TS_SYNC_BYTE:
                new_byte = stream_handle.read(1)
                if not new_byte:
                    ts_packet = b''
                    break
                ts_packet = ts_packet[1:] + new_byte  # shift everything over by 1 byte

            if not ts_packet:
                break

            program_id = ts_get_pid(ts_packet)
            if program_id == 0x0000:
                if len(segment_buffer) > 0:
                    segments_buffer.append(bytes(segment_buffer))
                    segment_buffer.clear()
            segment_buffer += ts_packet
