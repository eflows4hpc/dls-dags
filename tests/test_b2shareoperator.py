import tempfile
import unittest
from unittest.mock import Mock, patch

from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.utils.dates import days_ago
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType

from b2shareoperator import (
    B2ShareOperator,
    add_file,
    create_draft_record,
    download_file,
    get_file_list,
    get_object_md,
    get_objects,
    get_record_template,
    submit_draft,
)

DEFAULT_DATE = "2019-10-03"
TEST_DAG_ID = "test_my_custom_operator"
TEST_TASK_ID = "test"


class B2ShareOperatorTest(unittest.TestCase):
    def setUp(self):
        with DAG(
            TEST_DAG_ID,
            schedule_interval="@daily",
            default_args={"start_date": days_ago(2)},
            params={"oid": "111"},
        ) as dag:
            B2ShareOperator(task_id=TEST_TASK_ID, name="test_name")
        self.dag = dag
        # self.ti = TaskInstance(task=self.op, execution_date=days_ago(1))

    @unittest.skip("db structure changed")
    @patch("b2shareoperator.HttpHook")
    @patch("b2shareoperator.get_file_list")
    @patch("b2shareoperator.download_file")
    def test_alt_execute_no_trigger(self, down, gfl, ht):
        gfl.return_value = {"ooo.txt": "htt://file/to/download"}
        down.return_value = "tmp_name"

        dagrun = self.dag.create_dagrun(
            state=DagRunState.RUNNING,
            run_type=DagRunType.MANUAL,
            execution_date=days_ago(1),
        )

        ti = dagrun.get_task_instance(task_id=TEST_TASK_ID)
        ti.task = self.dag.get_task(task_id=TEST_TASK_ID)
        ti.run(ignore_ti_state=True)
        print(ti.state)

        self.assertEqual(State.SUCCESS, ti.state)

        # return value
        ret = ti.xcom_pull()
        self.assertEqual(ret, 1, f"{ret}")

        lcl = ti.xcom_pull(key="local")
        rmt = ti.xcom_pull(key="remote")
        mps = ti.xcom_pull(key="mappings")
        self.assertEqual(len(mps), 1, f"{mps}")
        self.assertDictEqual(
            mps, {"ooo.txt": "tmp_name"}, f"unexpecting mappings: {mps}"
        )
        self.assertEqual(lcl, "tmp_name", f"unexpecting local name: {lcl}")
        self.assertEqual(rmt, "ooo.txt", f"unexpected remote name: {rmt}")

    def test_get_files(self):
        with patch("b2shareoperator.requests.get") as get:
            m = Mock()
            m.json.return_value = {
                "contents": [
                    {"key": "veryimportant.txt", "links": {"self": "http://foo.bar"}}
                ]
            }
            get.return_value = m
            ret = get_file_list(obj={"links": {"files": ["bla"]}})
            self.assertEqual(len(ret), 1)

    def test_download_file(self):
        with patch("b2shareoperator.urllib.request.urlretrieve") as rr:
            with patch("b2shareoperator.tempfile.mktemp") as mt:
                mt.return_value = "/tmp/val"
                fname = download_file(url="http://foo.bar", target_dir="/no/tmp/")
                self.assertEqual(fname, "/tmp/val")

    def test_get_md(self):
        with patch("b2shareoperator.requests.get") as get:
            m = Mock()
            rval = {"links": {"files": ["a", "b"]}}
            m.json.return_value = rval
            get.return_value = m
            r = get_object_md(server="foo", oid="bar")
            self.assertDictEqual(rval, r)

    def test_get_objects(self):
        with patch("b2shareoperator.requests.get") as get:
            m = Mock()
            rval = {"hits": {"hits": ["a", "b"]}}
            m.json.return_value = rval
            get.return_value = m
            r = get_objects(server="foo")
            self.assertListEqual(["a", "b"], r)

    def test_upload(self):
        template = get_record_template()
        server = "https://b2share-testing.fz-juelich.de/"
        token = ""
        with patch("b2shareoperator.requests.post") as post:
            r = create_draft_record(server=server, token=token, record=template)

        r = dict()
        r["links"] = {"files": server, "self": server}
        with patch("b2shareoperator.requests.put") as put:
            a = tempfile.NamedTemporaryFile()
            a.write(b"some content")
            up = add_file(
                record=r, fname=a.name, token=token, remote="/tmp/somefile.txt"
            )

        with patch("b2shareoperator.requests.patch") as p:
            submitted = submit_draft(record=r, token=token)
