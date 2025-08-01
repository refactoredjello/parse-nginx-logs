import argparse
from collections import namedtuple
from datetime import datetime
from db import create_connection, insert_values, trunc_table, get_count

from tqdm import tqdm

# '172.60.244.120 - stephanie89 [03/feb/2006:07:19:57 +0000] "CONNECT /blog/categories/explore HTTP/1.1" 466 957 "https://www.phillips.info/category/blog/postsabout.html" "Opera/9.71.(X11; Linux x86_64; mn-MN) Presto/2.9.190 Version/12.00"'
# '$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"'
#
#              ' - '          ' ['        '] "'      '" '     ' '              ' "'           '" "'             '"'

# TODO
# 2. create report based on common attributes
# 3. allow a variable format that is parsed on the onset and detects delimiters automatically

delimiters = (" - ", " [", '] "', '" ', ' ', ' "', '" "', '"')
ParsedRequest = namedtuple("ParsedRequest", ["http_method", "path", "http_version"])
LogLine = namedtuple("Line", ["remote_addr", "user", "local_time", "request", "status", "bytes_sent", "http_referer",
                           "user_agent"])


def parse_line(line: str) -> list[str]:
    store = []

    i = 0  # start
    j = 0  # end
    k = 0  # delimiter and end

    while k < len(delimiters) and j <= len(line) - len(delimiters[k]):

        if line[j:j + len(delimiters[k])] == delimiters[k]:
            store.append(line[i:j])
            j += len(delimiters[k])
            i = j
            k += 1
        else:
            j += 1

    return store


# CONNECT /blog/categories/explore HTTP/1.1
def parse_request(request: str) -> ParsedRequest:
    parts = request.split(" ")
    return ParsedRequest(parts[0], parts[1], parts[2])


# 03/feb/2006:07:19:57 +0000
def to_datetime(raw_time: str):
    fmt = "%d/%b/%Y:%H:%M:%S %z"
    return datetime.strptime(raw_time, fmt)


def marshal_line(parsed_line: list[str]) -> LogLine:
    return LogLine(
        parsed_line[0],  # remote_addr
        parsed_line[1],  # remote_user
        to_datetime(parsed_line[2]),  # local_time
        parse_request(parsed_line[3]),  # request
        parsed_line[4],  # status
        parsed_line[5],  # bytes_sent
        parsed_line[6],  # referer
        parsed_line[7]  # user_agent
    )


def read_file(file_path, show_progress=True):
    t = tqdm(total=0, unit=" log lines", disable=not show_progress)
    with open(file_path, 'r') as f:
        for line in f:
            parsed_line = parse_line(line)
            t.update()
            yield marshal_line(parsed_line)

    t.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='Parse Nginx Logs')
    parser.add_argument(
        "--file", "-f",
        type=str,
        required=True,
        help="Absolute dir path of file to parse."
    )
    args = parser.parse_args()
    conn = create_connection("nginx_report.db")

    trunc_table(conn)
    print("Parsing logs...")
    for line in read_file(args.file):
        insert_values(conn, line)

    count = get_count(conn)
    print(f"Inserted {count} lines")
