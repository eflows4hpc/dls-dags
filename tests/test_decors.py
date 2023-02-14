import unittest
from unittest.mock import MagicMock, patch

from airflow import settings

from decors import create_temp_connection, get_connection, remove, setup


class TestDecors(unittest.TestCase):
    def test_create_con(self):
        con_id = create_temp_connection(rrid="foo", params={"host": "bar"})
        self.assertIsNotNone(con_id)
        self.assertEqual("tmp_connection_foo", con_id)

        session = settings.Session()
        res = session.execute(
            'Select * from connection where conn_id = "tmp_connection_foo"'
        )
        rows = res.fetchall()
        print(rows)
        self.assertEqual(1, len(rows))

        res = session.execute(
            'delete from connection where conn_id = "tmp_connection_foo"'
        )
        session.commit()

    @patch("decors.VaultHook")
    def test_connection_from_vault(self, vh):
        mm = MagicMock()
        mm.get_secret.return_value = {
            "privateKey": """-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAlwAAAAdzc2gtcn
NhAAAAAwEAAQAAAIEA7Tc2Z00KNBwatY1GJcgHlAefKyjgRxTOkWcJx2YXx3OLo5LOsXeN
FZu9sirCg0nX2wo1AHwrTmNXeSK8Jb1ToTqb2/bGqV15qwpyhOi3DFLTgCsPr/TdniNa0Z
M/aOLmyGkStR92l1wiQpdjKKTYINkewDey3f8iEyM1GJUuTqcAAAIAi/LWbovy1m4AAAAH
c3NoLXJzYQAAAIEA7Tc2Z00KNBwatY1GJcgHlAefKyjgRxTOkWcJx2YXx3OLo5LOsXeNFZ
u9sirCg0nX2wo1AHwrTmNXeSK8Jb1ToTqb2/bGqV15qwpyhOi3DFLTgCsPr/TdniNa0ZM/
aOLmyGkStR92l1wiQpdjKKTYINkewDey3f8iEyM1GJUuTqcAAAADAQABAAAAgHmrDf5h50
/e8lwYP9h1Bl4IorzyVEZhB6pQXRWm6Z9lRkP5soYXuYdRwDQ39lh8cXhpfdHn3hZwoZcC
F9qLhu54ZHWZ33yWxfSjRuzLn3BBYUeJ178d65puBASe8wce1kbTctJkF6wVYveQt9gbNb
fkHVzioMUXeATsdP5LrCbBAAAAQBP5Rgb0QJT2xZB6pnE64Ah8c6jquOJcq2jvOHziIFyW
PbEEqCWjRNjwHZv6J4NyYZNltNFbHNdi2vYX5ON/ehkAAABBAPbwyPL48/tMT+KQP7tBhH
mkejeeEBJUbuFStOCsgFP+kXz8joW7+Dx2/9lIarj50L0kGKXhw7Tq2LMOM35ippcAAABB
APXrF+bW90YpbkcJP2px+4rhfJUee3vK0Mc+wj566vsp6WElK6aF7wGUCk+1kPrL/viWEl
wFWYfCPtYhDi3iKnEAAAAJampAamotOTA4AQI=
-----END OPENSSH PRIVATE KEY-----"""
        }
        vh.return_value = mm
        conn = get_connection(conn_id="vault_4044", params={"host": "bar"})
        self.assertIsNotNone(conn)
        self.assertEqual("bar", conn.remote_host)

    @patch("decors.SSHHook")
    def test_temp_conn(self, hook):
        conn = get_connection(conn_id="random_conn", params={"host": "bar"})
        self.assertIsNotNone(conn)

    @patch("decors.create_temp_connection")
    def test_setup(self, tmpc):
        setup(params={}, run_id=42)
        tmpc.assert_called()

        ret = setup(params={"vault_id": 888})
        self.assertTrue(ret.startswith("vault_"))

    def test_remove(self):
        con_id = create_temp_connection(rrid="foo", params={"host": "bar"})
        self.assertIsNotNone(con_id)
        self.assertEqual("tmp_connection_foo", con_id)
        session = settings.Session()
        res = session.execute(
            'Select * from connection where conn_id = "tmp_connection_foo"'
        )
        self.assertEqual(1, len(res.fetchall()))

        remove(conn_id=con_id)
        session = settings.Session()
        res = session.execute(
            'Select * from connection where conn_id = "tmp_connection_foo"'
        )
        rows = res.fetchall()
        self.assertEqual(0, len(rows))

        remove(conn_id="vault_888")
