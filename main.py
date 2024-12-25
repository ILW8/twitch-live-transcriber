import datetime
import json
import logging
import os
import socket
import threading
import time
from json import JSONDecodeError

import streamlink
import ffmpeg
from id3parse import ID3, TAG_HEADER_SIZE
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


TS_PACKET_LENGTH = 188
TS_SYNC_BYTE = 0x47

PMT_STREAM_TYPES = {
    0x01: "MPEG-1 Video",
    0x02: "MPEG-2 Video",
    0x03: "MPEG-1 Audio",
    0x04: "MPEG-2 Audio",
    0x0F: "AAC Audio",
    0x15: "ID3 metadata",
    0x1B: "H.264 Video",
    0x24: "H.265/HEVC Video"
}


class SegmentWithMeta:
    def __init__(self, segment):
        self.segment = segment

        ffmpeg_id3_extract = (ffmpeg.input("pipe:", f="mpegts")['d:0']
                                    .output("pipe:", f="data")
                                    .run_async(pipe_stdin=True, pipe_stdout=True, quiet=True))
        stdout, stderr = ffmpeg_id3_extract.communicate(input=segment)

        if "Packet corrupt" in (stderr := stderr.decode('utf-8')) or 'matches no streams' in stderr:
            raise ValueError("unable to parse segment or something")

        twitch_txxx_segment_meta = None
        while len(stdout) > 0:
            id3data = ID3.from_byte_array(stdout)
            try:
                twitch_txxx_segment_meta = json.loads(id3data.find_frame_by_name("TXXX")
                                                      .raw_bytes[17:]
                                                      .decode('utf-8'))
                logger.info(f"size: {len(segment)} stream timestamp: "
                            f"{datetime.timedelta(seconds=twitch_txxx_segment_meta['stream_offset'])}")
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


class NetworkAudioSender:
    # assuming f32le pcm audio
    bytes_per_sample = 4

    def __init__(self, host: str, port: int, sample_rate : int = 16_000):
        self.sample_rate = sample_rate
        self.step_length = 200  # ms
        self.time_since_last_data = None
        self.steps_buffer = []
        self.partial_step_buffer = bytearray()

    def __enter__(self):
        logger.info("connecting to whisper server...")
        # self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.sock.connect((host, port))
        logger.info("connected to whisper server")
        self.stop = False
        self.sender_thread = threading.Thread(target=self.send_audio_thread)
        self.sender_thread.start()

    def __exit__(self, exc_type, exc_value, tb):
        # self.sock.close()
        self.stop = True
        self.sender_thread.join()

    def queue_audio(self, audio_data: bytes):
        assert(len(audio_data) % self.bytes_per_sample == 0)
        logger.debug(f"queued audio data: {len(audio_data)} bytes ({len(audio_data) // self.bytes_per_sample} samples)")

        if len(self.partial_step_buffer) > 0:
            audio_data = self.partial_step_buffer + audio_data
            self.partial_step_buffer.clear()

        # split audio into steps
        offset = 0

        # exclude the end if it there isn't enough data to fill a whole step
        while offset <= len(audio_data) - ((self.step_length * self.sample_rate) // 1000) * self.bytes_per_sample:
            step = audio_data[offset:offset + self.step_length * self.bytes_per_sample]
            self.steps_buffer.append(step)
            offset += len(step)

    def send_audio_thread(self):
        while not self.stop:
            print("hello, this is the sender thread")
            time.sleep(1)


class CircularSegmentBuffer:
    def __init__(self, network_audio = None, max_segment_count=5):
        self.segments: [SegmentWithMeta] = []
        self.max_segment_count = max_segment_count
        self.network_audio = network_audio

    def append(self, segment: bytes):
        if len(self.segments) >= self.max_segment_count:
            self.pop()
        self.segments.append(SegmentWithMeta(segment))

        # i don't remember why I made this circular buffer for, but im just going to do the audio sending here
        # send in pcm_s16le format at 16kHz
        ffmpeg_prep_audio = (ffmpeg.input("pipe:", f="mpegts")['a:0']
                                   .output("pipe:", f="f32le", ac=1, ar=16000)
                                   .run_async(pipe_stdin=True, pipe_stdout=True, quiet=True))
        stdout: bytes
        stderr: bytes | str
        stdout, stderr = ffmpeg_prep_audio.communicate(input=segment)

        if "Packet corrupt" in (stderr := stderr.decode('utf-8')) or 'matches no streams' in stderr:
            return

        if self.network_audio is not None:
            self.network_audio.queue_audio(stdout)

    def pop(self):
        return self.segments.pop(0)


def ts_get_pid(packet: bytes) -> int:
    if len(packet) != TS_PACKET_LENGTH:
        logger.error("dkljasdhjklasfdklfsdjklhasfdhklj")
    assert len(packet) == TS_PACKET_LENGTH
    return ((packet[1] & 0x1F) << 8) | (packet[2] & 0xFF)

def ts_get_payload_start_bit(packet: bytes):
    return (packet[0] & 0x40) >> 6


def parse_pat(packet: bytes):
    # Skip pointer field
    pointer_field = packet[0]
    pat_data = packet[1 + pointer_field:]

    # Verify table_id
    table_id = pat_data[0]
    if table_id != 0x00:
        raise ValueError("Not a PAT table")

    # Section length (12 bits)
    section_length = ((pat_data[1] & 0x0F) << 8) | pat_data[2]
    program_data_length = section_length - 9  # Exclude header (9 bytes) and CRC32 (4 bytes)

    # Loop through program data
    program_info = []
    index = 8  # Program info starts after the 8-byte header
    while index < 8 + program_data_length:
        program_number = (pat_data[index] << 8) | pat_data[index + 1]
        program_map_pid = ((pat_data[index + 2] & 0x1F) << 8) | pat_data[index + 3]
        program_info.append((program_number, program_map_pid))
        index += 4

    return program_info


def parse_pmt(packet: bytes):
    # Skip pointer field
    pointer_field = packet[0]
    pmt_data = packet[1 + pointer_field:]

    table_id = pmt_data[0]
    if table_id != 0x02:
        raise ValueError("Not a PMT table")

    section_length = ((pmt_data[1] & 0x0F) << 8) | pmt_data[2]
    # pcr_pid = ((pmt_data[8] & 0x1F) << 8) | pmt_data[9]
    program_info_length = ((pmt_data[10] & 0x0F) << 8) | pmt_data[11]
    program_info_end = 12 + program_info_length

    # Parse the stream loop
    stream_info = []
    index = program_info_end
    while index < 3 + section_length - 4:  # Exclude CRC
        stream_type = pmt_data[index]
        elementary_pid = ((pmt_data[index + 1] & 0x1F) << 8) | pmt_data[index + 2]
        es_info_length = ((pmt_data[index + 3] & 0x0F) << 8) | pmt_data[index + 4]
        stream_info.append((stream_type, elementary_pid))
        index += 5 + es_info_length

    return stream_info



"""
https://arca.live/b/twitchdev/48875066
https://web.archive.org/save/https://arca-live.translate.goog/b/twitchdev/48875066?_x_tr_sl=auto&_x_tr_tl=en&_x_tr_hl=en&_x_tr_pto=wapp
"""


def main(network_audio: NetworkAudioSender|None = None):
    options = {
        "low-latency": True,
    }
    if os.getenv("TWITCH_TOKEN") is not None:
        options["api-header"] = [("Authorization", f"OAuth {os.getenv('TWITCH_TOKEN')}")]

    logger.info("fetching stream info...")
    streams = streamlink.streams("https://twitch.tv/btmc", options=options)

    logger.info("preparing audio stream...")
    audio_stream = streams.get("audio_only", None)
    assert audio_stream is not None

    pmt_pid = None
    segments_queue = CircularSegmentBuffer(network_audio)
    with audio_stream.open() as stream_handle:
        logger.info("opened audio stream")
        segment_buffer = bytearray()
        ts_packet_buffer = bytearray()
        while True:
            while len(ts_packet_buffer) < TS_PACKET_LENGTH:
                ts_packet = stream_handle.read(TS_PACKET_LENGTH)
                if not ts_packet:
                    break
                ts_packet_buffer.extend(ts_packet)

            # todo: remove this, currently here to maintain compatibility
            ts_packet = bytes(ts_packet_buffer[:TS_PACKET_LENGTH])
            ts_packet_buffer = ts_packet_buffer[TS_PACKET_LENGTH:]

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
            payload_unit_start_indicator = ts_get_payload_start_bit(ts_packet)

            do_flush_segment_buffer = False

            if program_id == 0x0000 and payload_unit_start_indicator == 1:
                # parse PAT for stream IDs
                res = parse_pat(ts_packet[4:])
                assert len(res) == 1

                # program id: program index
                # program_map_pid: PMT PID
                _, pmt_pid = res[0]
                logger.debug(f"program_id: {program_id} program_map_pid: {pmt_pid}")

                if len(segment_buffer) > 0:
                    do_flush_segment_buffer = True

            if pmt_pid is not None and program_id == pmt_pid:
                # parse PMT for audio stream ID
                logger.debug(f"got packet with PMT PID: {pmt_pid}")
                res = parse_pmt(ts_packet[4:])
                for stream_type, elementary_pid in res:
                    if stream_type in PMT_STREAM_TYPES:
                        logger.debug(f"found stream type: {PMT_STREAM_TYPES[stream_type]} with PID: {elementary_pid}")
                        continue

                    logger.info(f"unknown stream type: {stream_type} with PID: {elementary_pid}")
                pass

            if do_flush_segment_buffer:
                segments_queue.append(bytes(segment_buffer))
                segment_buffer.clear()
            segment_buffer += ts_packet


if __name__ == '__main__':
    load_dotenv()

    if os.getenv("SEND_TO_WHISPER", None) is not None:
        with NetworkAudioSender("127.0.0.1", 9727) as net_audio_sender:
            main(net_audio_sender)
    else:
        main()
