import importlib
import pathlib
import select
import shutil
import socket
import ssl
import struct
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, str(pathlib.Path(__file__).parent / "validation"))
wlclient = importlib.import_module("wlclient")
wlserver = importlib.import_module("wlserver")


@unittest.skipUnless(shutil.which("openssl"), "needs openssl for the throwaway certificate")
class ConcurrentTLSTest(unittest.TestCase):
    """A terminal session reads echoes in one thread while it types in another.
    On a bare SSLSocket that returns spurious EOFs and "bad record mac" errors
    (or crashes) in about one session in ten here; through serial() none."""

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()

        class Backlog(wlserver.Server):
            request_queue_size = 128

        self.server = Backlog(("127.0.0.1", 0), wlserver.Term, wlserver.tls_context(self.dir.name))
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.dir.cleanup()

    def session(self, _):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        for attempt in range(5):            # a full accept queue resets a connect
            try:
                raw = socket.create_connection(self.server.server_address, timeout=10)
                break
            except ConnectionResetError:
                if attempt == 4:
                    raise
                time.sleep(0.2)
        ss = ctx.wrap_socket(raw, server_hostname="wlserver", do_handshake_on_connect=False)
        ss.setblocking(False)              # the handshake as open_conn does it
        while True:
            try:
                ss.do_handshake()
                break
            except ssl.SSLWantReadError:
                select.select([ss], [], [], 10)
            except ssl.SSLWantWriteError:
                select.select([], [ss], [], 10)
        ss.settimeout(10)
        sock = wlclient.serial(ss)
        echoes, fail = [], []

        def reader():
            r = wlclient.Reader(sock)
            try:
                while len(echoes) < 5:
                    _, n = struct.unpack(">cH", r.exact(3))
                    echoes.append(struct.unpack(">I", r.exact(n)[:4])[0])
            except (OSError, EOFError, ssl.SSLError) as e:
                fail.append(f"{type(e).__name__}: {e}")

        th = threading.Thread(target=reader)
        th.start()
        for seq in range(5):
            sock.sendall(b"K" + struct.pack(">H", 36) + struct.pack(">I", seq) + b"k" * 32)
        th.join(15)
        sock.close()
        return fail[0] if fail else echoes

    def test_a_reader_and_a_writer_share_one_tls_connection(self):
        with ThreadPoolExecutor(8) as ex:
            results = list(ex.map(self.session, range(300)))
        self.assertEqual([r for r in results if r != [0, 1, 2, 3, 4]], [])

    def test_a_plain_socket_is_left_as_it_is(self):
        s = socket.socket()
        try:
            self.assertIs(wlclient.serial(s), s)
        finally:
            s.close()


if __name__ == "__main__":
    unittest.main()
