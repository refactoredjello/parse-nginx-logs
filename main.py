from collections import namedtuple

# line = '172.60.244.120 - stephanie89 [2005-02-03:07:19:57 +0000] "CONNECT /blog/categories/explore HTTP/1.1" 466 957 "https://www.phillips.info/category/blog/postsabout.html" "Opera/9.71.(X11; Linux x86_64; mn-MN) Presto/2.9.190 Version/12.00"'
# '$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"'
#
#              ' - '          ' ['        '] "'      '" '     ' '              ' "'           '" "'             '"'

# TODO
# 1. local_time to date time?
# 2. create report based on common attributes
# 3. allow a variable format that is parsed on the onset and detects delimiters automatically

delimiters = (" - ", " [", '] "', '" ', ' ', ' "', '" "', '"')
ParsedRequest = namedtuple("ParsedRequest", ["http_method", "path", "http_version"])
Line = namedtuple("Line", ["remote_addr", "user", "local_time", "request", "status", "bytes_sent", "http_referer", "user_agent"])


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



def marshall_line(parsed_line: list[str]) -> Line:
    return Line(parsed_line[0], parsed_line[1], parsed_line[2], parse_request(parsed_line[3]), parsed_line[4],parsed_line[5], parsed_line[6], parsed_line[7])

def read_file(file_path):
    with open(file_path, 'r') as f:
        for line in f:
            parsed_line = parse_line(line)
            yield marshall_line(parsed_line)



if __name__ == "__main__":
    for line in read_file("/Users/gil/dev/fake-log-gen/output/fake.log"):
        print(line)

