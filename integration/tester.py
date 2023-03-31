import json
import subprocess
import re

from argparse import ArgumentParser


def get_config(host, port, login, target):

    return {
        "force": True,
        "url": "https://raw.githubusercontent.com/eflows4hpc/dls-dags/master/integration/data.txt",
        "host": host,
        "target": target,
        "login": login,
        "port": port,
        "key": "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAlwAAAAdzc2gtcn\nNhAAAAAwEAAQAAAIEAuq+ua51m1MHDqdSgZ2w/oJOcQrAyXMUUAtDNi7/OyyrVDMqHp3rR\nuNGCuzKhztwwQ8ATtoHtQADH/CYw8/T1EtaDPSvqZRmWHEujPTwsrgtulpRkBcaMO35lB2\nfENBMLRgA9cmmyTcduTqnbYL7UdMKoYhcw0yTgLlNK9H0vnbUAAAIA+0Tb/ftE2/0AAAAH\nc3NoLXJzYQAAAIEAuq+ua51m1MHDqdSgZ2w/oJOcQrAyXMUUAtDNi7/OyyrVDMqHp3rRuN\nGCuzKhztwwQ8ATtoHtQADH/CYw8/T1EtaDPSvqZRmWHEujPTwsrgtulpRkBcaMO35lB2fE\nNBMLRgA9cmmyTcduTqnbYL7UdMKoYhcw0yTgLlNK9H0vnbUAAAADAQABAAAAgQCvBjSNuk\nWFZKBP4gP80rUYlCulLlIZPb/UH/UFd2+mdOLHmj3yXCixkONzJDYlnbQ2YKdarZdEMTdN\nhHTS067LPzBJ0wOwt+Q2ZoVnJ//Glp1JoYX4ldEfGT0Ihmqn3wJscHwIGeWbM4AD/ZIa9H\n8BnMWq8d4x7W4aLOd11xxpYQAAAEALdgo2Ti1lmq9rmyAj8s5OjeJxwzdFUsuMPAGgD+D4\nqowTAehb7aLqkjtZyJn2HliLVODrNohW+uFsciQsaRLmAAAAQQDkycH+AJ7kHW4wcb8RLr\n/HU52EVKIcCv0bwqwnATKQIbz8fqCldhdD8O47rpBsD7Iyvp9PDjPFTiBXhBfqwGydAAAA\nQQDQ4/zEgPFngsuI+e4j1H4oSmLgAXsw1kcDNYq7a/JK7R7kGnIhPX7tI0qiFrq0h9iKip\niKOaW0KG6obaAX6g35AAAACWpqQGpqLTkwOAE=\n-----END OPENSSH PRIVATE KEY-----\n",
    }


if __name__ == "__main__":

    parser = ArgumentParser(
        prog="DAG tester", description="Tests dags in integration env"
    )

    parser.add_argument("--host", default="localhost")
    parser.add_argument("-p", "--port", type=int, default=2222)
    parser.add_argument("-u", "--user", default="eflows")
    parser.add_argument("--target", default="/tmp/myfile")

    args = parser.parse_args()

    conf = get_config(
        host=args.host, port=args.port, login=args.user, target=args.target
    )
    cfgstr = json.dumps(conf)

    res = subprocess.run(
        ["airflow", "dags", "test", "plainhttp2ssh", "-c", cfgstr], capture_output=True
    )
    print(res)
    print("\n" * 2)

    assert res.returncode == 0

    s = res.stdout.decode()

    bts = int(re.findall("Written (\d+)", s)[0])
    assert bts == 1669
