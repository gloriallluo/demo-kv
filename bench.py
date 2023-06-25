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
    parser.add_argument("-t", "--tests", type=int, nargs="+", choices=range(4, 6),
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
    command = [f"./target/{mode}/server"]
    return Popen(command, shell=True)


def start_client(mode: str, input_file: Path) -> Popen:
    command = [f"./target/{mode}/client"]
    with open(input_file, "r") as f:
        return Popen(command, shell=True, stdin=f)


def start_bench(mode: str, name: str) -> Popen:
    command = [f"./target/{mode}/{name}"]
    return Popen(command, shell=True)


if __name__ == '__main__':
    args = parse_args()
    test_dir = Path(args.dir)
    assert test_dir.is_dir()

    tests = range(1, 7) if args.all else args.tests

    serv = start_server(args.mode)
    time.sleep(1)

    # test 4
    if 4 in args.tests:
        f4 = get_test_files(test_dir, 4)
        c0 = start_client(args.mode, test_dir / "testcase4-1.txt")
        c0.wait()
        clients = []
        for i in range(3):
            clients.append(start_bench(args.mode, "write0"))
        time.sleep(60)
        for p in clients:
            p.kill()
        time.sleep(2)
        c1 = start_client(args.mode, test_dir / "testcase4-3.txt")
        c1.wait()

    # test 5
    if 5 in args.tests:
        f5 = get_test_files(test_dir, 5)
        c0 = start_client(args.mode, test_dir / "testcase5-1.txt")
        c0.wait()
        clients = []
        for i in range(2):
            clients.append(start_bench(args.mode, "write1"))
        for i in range(8):
            clients.append(start_bench(args.mode, "read1"))
        time.sleep(60)
        for p in clients:
            p.kill()
        time.sleep(2)
        c1 = start_client(args.mode, test_dir / "testcase5-4.txt")
        c1.wait()

    serv.kill()
    subprocess.run("killall client", shell=True, stdout=subprocess.DEVNULL)
    subprocess.run("killall server", shell=True, stdout=subprocess.DEVNULL)
