"""
Script to get UDP traffic and save it to a compressed file on an S3 bucket,
including source IP (will break if ipv6 is used) and timestamp

Run as python trafficdumper.py PORT BUCKETNAME
"""
import logging
import select
from botocore.exceptions import ClientError
import time
import struct
import socket
import zlib
import datetime
import constants
import boto3
import sys
logger = logging.getLogger()


S3_BUCKET = sys.argv[2]
PREFIX = sys.argv[3]
assert PREFIX == "" or PREFIX.endswith("/")
BIND_ADDRESS = "0.0.0.0"
BIND_PORT = int(sys.argv[1])
SAVE_INTERVAL = constants.MINUTE_TD
SOCKET_LISTEN_INTERVAL_SECONDS = 10


def todayutc():
    return datetime.datetime.utcnow().date()

s3 = boto3.resource('s3')
today_date = todayutc()
last_save = datetime.datetime.utcnow()


compressed_data = None
compressor = None
compressedlines = None
lines = None


class Decompressor:
    def __init__(self, stream):
        self.stream = stream
        self.decompressobj = zlib.decompressobj()
        self.decompressed_tail = b""

    def read(self, nrbytes):
        decompressed = b""
        nrbytes_needed = nrbytes
        while True:
            decompressed += self.decompressed_tail[:nrbytes_needed]
            self.decompressed_tail = self.decompressed_tail[nrbytes_needed:]
            nrbytes_needed = nrbytes - len(decompressed)
            if nrbytes_needed == 0:
                return decompressed
            data = self.stream.read(1500)
            self.decompressed_tail += self.decompressobj.decompress(data)

    def is_eof(self):
        return self.decompressobj.eof


def init_today_state():
    globals()["compressed_data"] = b""
    globals()["compressor"] = zlib.compressobj(level=9)
    globals()["compressedlines"] = 0
    globals()["lines"] = []


def process_line(sock):
    buf, (ip, port) = sock.recvfrom(2**16 - 1)
    src = sum(map(lambda el: int(el[1]) * 8**(3-el[0]),
                  enumerate(ip.split("."))))
    receivedtime = time.time()
    lines.append(struct.pack("!HId", len(buf) + 14, src, receivedtime) + buf)
    logger.info("Received line from %s, now %d lines in memory "
                "(%d lines since last save)",
                ip, len(lines) + compressedlines, len(lines))


def skip_day():
    intermediate_save(force=True)
    globals()["today_date"] = todayutc()
    init_today_state()
    logger.info("New day: %s", str(today_date))


def get_object_today():
    filename = today_date.strftime(PREFIX + "%Y/%Y-%m-%d-%a.capture.gz")
    return s3.Object(S3_BUCKET, filename)


def load():
    try:
        response = get_object_today().get()
    except ClientError as e:
        if e.response['Error'].get('Code') in ("NoSuchKey", "AccessDenied"):
            # Note: Assumes rights are in order. In that case, if no listing
            # rights are given, AccessDenied will be returned for non-
            # existing objects
            logger.warning("Couldn't find previous file for today, "
                           "using empty one")
        else:
            raise
    else:
        bodystream = Decompressor(response["Body"])
        assert compressedlines == 0
        while not bodystream.is_eof():
            linelenbytes = bodystream.read(2)
            linelen = struct.unpack("!H", linelenbytes)[0]
            assert linelen <= 1510, "corrupt?"
            line = linelenbytes + bodystream.read(linelen - 2)
            assert linelen == len(line)
            globals()["compressed_data"] += compressor.compress(line)
            globals()["compressedlines"] += 1
        logger.info("Imported %d lines", compressedlines)


def intermediate_save(force):
    if not lines and not force:
        globals()["last_save"] = datetime.datetime.utcnow()
        logger.info("Not saving since now new data")
        return
    globals()["compressed_data"] += compressor.compress(b"".join(lines))
    body = compressed_data + compressor.copy().flush()

    s3object = get_object_today()
    s3object.put(Body=body)
    logger.info("Saved %s, %d bytes", s3object.key, len(body))
    globals()["last_save"] = datetime.datetime.utcnow()
    globals()["compressedlines"] += len(lines)
    globals()["lines"] = []


def run():
    init_today_state()
    load()
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind((BIND_ADDRESS, BIND_PORT))

            while True:
                ready = select.select(
                    [sock], [], [], SOCKET_LISTEN_INTERVAL_SECONDS)
                if ready[0]:
                    process_line(sock)
                if today_date != todayutc():
                    skip_day()
                if datetime.datetime.utcnow() - last_save > SAVE_INTERVAL:
                    intermediate_save(force=False)

        except Exception:
            logger.exception("Error")
        finally:
            try:
                sock.close()
            except:
                pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('nose').setLevel(logging.WARNING)
    run()
