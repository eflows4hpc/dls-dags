import unittest

# from unittest.mock import MagicMock, create_autospec, patch
from gitlab2ssh import get_gitlab_client, get_project, GitFSC
from utils import walk_dir

URL = "https://gitlab.jsc.fz-juelich.de/"


class TestGits(unittest.TestCase):
    def test_get_client(self):
        client = get_gitlab_client(url=URL)
        self.assertIsNotNone(client)

    def test_get_project(self):
        client = get_gitlab_client(url=URL)
        project = get_project(client=client, name="ModelRepository")
        self.assertIsNotNone(project)

        print("Got project", project.name)

    def test_gitF(self):
        client = get_gitlab_client(url=URL)
        project = get_project(client=client, name="ModelRepository")

        gitf = GitFSC(client=project)
        files = gitf.list(path="deployment", get_info=False)
        self.assertIsNotNone(files)
        self.assertTrue(len(files) > 0)
        print("Got files: ", files)

        self.assertTrue("docker-compose.yml" in files)

    def test_walkdir(self):
        client = get_gitlab_client(url=URL)
        project = get_project(client=client, name="ModelRepository")

        gitf = GitFSC(client=project)
        files = list(walk_dir(client=gitf, path="", prefix=""))
        self.assertIsNotNone(files)
        self.assertTrue(len(files) > 0)

        self.assertTrue("deployment/Dockerfile" in files)
