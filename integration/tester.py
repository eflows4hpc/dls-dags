import json
import subprocess
import re

def get_config():

    return {
        'force': True,
        'url': 'https://raw.githubusercontent.com/eflows4hpc/dls-dags/master/README.md',
        'host': 'openssh-server',
        'target': '/tmp/myfile',
        'login': 'eflows',
        'port': 2222,
        'key': '-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAlwAAAAdzc2gtcn\nNhAAAAAwEAAQAAAIEAuq+ua51m1MHDqdSgZ2w/oJOcQrAyXMUUAtDNi7/OyyrVDMqHp3rR\nuNGCuzKhztwwQ8ATtoHtQADH/CYw8/T1EtaDPSvqZRmWHEujPTwsrgtulpRkBcaMO35lB2\nfENBMLRgA9cmmyTcduTqnbYL7UdMKoYhcw0yTgLlNK9H0vnbUAAAIA+0Tb/ftE2/0AAAAH\nc3NoLXJzYQAAAIEAuq+ua51m1MHDqdSgZ2w/oJOcQrAyXMUUAtDNi7/OyyrVDMqHp3rRuN\nGCuzKhztwwQ8ATtoHtQADH/CYw8/T1EtaDPSvqZRmWHEujPTwsrgtulpRkBcaMO35lB2fE\nNBMLRgA9cmmyTcduTqnbYL7UdMKoYhcw0yTgLlNK9H0vnbUAAAADAQABAAAAgQCvBjSNuk\nWFZKBP4gP80rUYlCulLlIZPb/UH/UFd2+mdOLHmj3yXCixkONzJDYlnbQ2YKdarZdEMTdN\nhHTS067LPzBJ0wOwt+Q2ZoVnJ//Glp1JoYX4ldEfGT0Ihmqn3wJscHwIGeWbM4AD/ZIa9H\n8BnMWq8d4x7W4aLOd11xxpYQAAAEALdgo2Ti1lmq9rmyAj8s5OjeJxwzdFUsuMPAGgD+D4\nqowTAehb7aLqkjtZyJn2HliLVODrNohW+uFsciQsaRLmAAAAQQDkycH+AJ7kHW4wcb8RLr\n/HU52EVKIcCv0bwqwnATKQIbz8fqCldhdD8O47rpBsD7Iyvp9PDjPFTiBXhBfqwGydAAAA\nQQDQ4/zEgPFngsuI+e4j1H4oSmLgAXsw1kcDNYq7a/JK7R7kGnIhPX7tI0qiFrq0h9iKip\niKOaW0KG6obaAX6g35AAAACWpqQGpqLTkwOAE=\n-----END OPENSSH PRIVATE KEY-----\n'
       }


if __name__ == "__main__":
    conf = get_config()
    cfgstr = json.dumps(conf)

    res = subprocess.run(['airflow', 'dags', 'test' , 'plainhttp2ssh', '-c', cfgstr], capture_output=True)
    print(res)
    print('\n'*2)

    assert res.returncode == 0

    s = res.stdout.decode()

    bts = int(re.findall('Written (\d+)', s)[0])
    assert bts == 649


