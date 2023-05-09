#! python3

import argparse
import subprocess
import time
from pathlib import Path
from subprocess import Popen


def parse_args():
    parser = argparse.ArgumentParser(description="A script to run tests")
    parser.add_argument("-a", "--all", action="store_true",
                        help="test all testcases")
    parser.add_argument("-t", "--tests", type=int, nargs="+", choices=range(1, 7),
                        help="specify what testcases to run, enabled if `all` is unset")
    parser.add_argument("-d", "--dir", default="./testcases",
                        help="path to place testcase files")
    parser.add_argument("-m", "--mode", default="release", choices=["debug", "release"],
                        help="build mode")
    return parser.parse_args()


def get_test_files(test_dir: Path, test_id: int) -> [str]:
    files = list(test_dir.glob(f"**/testcase{test_id}*.txt"))
    return [f.name for f in files]


def start_server(mode: str) -> Popen:
    command = [f"./target/{mode}/demo-serv"]
    return Popen(command, shell=True)


def start_client(mode: str, input_file: Path) -> Popen:
    command = [f"./target/{mode}/demo-cli"]
    with open(input_file, "r") as f:
        return Popen(command, shell=True, stdin=f)


if __name__ == '__main__':
    args = parse_args()
    test_dir = Path(args.dir)
    assert test_dir.is_dir()

    tests = range(1, 7) if args.all else args.tests

    serv = start_server(args.mode)
    time.sleep(0.2)

    for tid in tests:
        files = get_test_files(test_dir, tid)
        print(f"tid={tid}, testcase={files}")
        clients = [start_client(args.mode, test_dir / f) for f in files]
        for c in clients:
            c.wait()

    serv.kill()  # XXX not working

    subprocess.run("killall demo-cli", shell=True)
    subprocess.run("killall demo-serv", shell=True)
