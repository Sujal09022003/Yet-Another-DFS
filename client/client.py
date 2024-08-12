import os

from client_utils import *
 
MASTER_NODE = os.getenv("MASTER_NODE", "http://localhost:3030/")


def status(*unused):
    resp = requests.get(os.path.join(MASTER_NODE, "status"))
    check_response(resp, "status", pretty_print_enabled=True)

def copy_file(*args):
    if check_args("copy", args, required_operands=["file", "target_path"]):
        fpath = make_abs(args[1])
        resp = requests.get(os.path.join(MASTER_NODE, f"file?filename={fpath}"))
        if check_response(resp, "download", print_content=False):
            data = resp.json()
            resp = request_datanodes(
                data["file"]["nodes"], f"file?filename={data['file']['file_id']}", "GET"
            )
            if check_response(resp, "download", print_content=False):
                data = resp.content
                path = make_abs(args[2])

                # Request to store a file in the filesystem
                # Request structure: /file?filename=<path>
                resp = requests.post(os.path.join(MASTER_NODE, f"file?filename={path}"))
                if check_response(resp, "copy", print_content=False):
                    content = resp.json()
                    nodes = content["datanodes"]  # Available storage datanodes
                    file = content[
                        "file"
                    ]  # View of a file from the perspective of a masternode
                    if data:
                        # Request to store a file in the storage
                        # Request structure: /file?filename=<filename>
                        request_datanodes(
                            nodes, f'file?filename={file["file_id"]}', "POST", data=data
                        )
def put_file(*args):
    if check_args("upload", args, required_operands=["file", "destination"]):
        fpath = args[1]
        data = os_read_file(fpath)
        if data:
            destination = make_abs(args[2])
            filename = os.path.basename(fpath)
            path = join_path(filename, destination)

            # Request to store a file in the filesystem
            # Request structure: /file?filename=<path>
            resp = requests.post(os.path.join(MASTER_NODE, f"file?filename={path}"))
            if check_response(resp, "upload", print_content=False):
                content = resp.json()
                nodes = content["datanodes"]  # Available storage datanodes
                file = content[
                    "file"
                ]  # View of a file from the perspective of a masternode
                if data:
                    # Request to store a file in the storage
                    # Request structure: /file?filename=<filename>
                    request_datanodes(
                        nodes, f'file?filename={file["file_id"]}', "POST", data=data
                    )


def change_dir(*args):
    if check_args("cd", args, required_operands=["destination"]):
        destination = make_abs(args[1])
        resp = requests.get(os.path.join(MASTER_NODE, f"directory?name={destination}"))
        if check_response(resp, "cd", verbose=False):
            set_pwd(destination)
        else:
            print(f"cd: {destination}: No such file or directory")


def make_dir(*args):
    if check_args("mkdir", args, required_operands=["destination"]):
        destination = make_abs(args[1])
        resp = requests.post(os.path.join(MASTER_NODE, f"directory?name={destination}"))
        check_response(resp, "mkdir")


def read_file(*args):
    if check_args("download", args, ["file", "local_destination"]):
        fpath = make_abs(args[1])
        dest = args[2]
        resp = requests.get(os.path.join(MASTER_NODE, f"file?filename={fpath}"))
        if check_response(resp, "download", print_content=False):
            data = resp.json()
            resp = request_datanodes(
                data["file"]["nodes"], f"file?filename={data['file']['file_id']}", "GET"
            )
            if check_response(resp, "get", print_content=False):
                print(f"File '{fpath}' successfully retrieved")
                print(f"Saving to '{dest}'")
                try:
                    f = open(dest, "wb")
                    f.write(resp.content)
                    print("Successfully saved")
                except OSError as e:
                    print(
                        f"Error while saving on local filesystem: {e.strerror} '{e.filename}'"
                    )


def remove_file_or_dir(*args):
    if check_args("delete", args, ["file_or_dir"]):
        destination = make_abs(args[1])

        if get_pwd().startswith(destination):
            print(
                f"delete: cannot remove '{destination}': It is a prefix of the current working directory"
            )
            return

        dir_resp = requests.get(
            os.path.join(MASTER_NODE, f"directory?name={destination}")
        )
        file_resp = requests.get(
            os.path.join(MASTER_NODE, f"file?filename={destination}")
        )

        if check_response(file_resp, verbose=False):
            resp = requests.delete(
                os.path.join(MASTER_NODE, f"file?filename={destination}")
            )
            check_response(resp, "delete")
        elif check_response(dir_resp, verbose=False):
            data = dir_resp.json()
            if (
                    len(data["dirs"]) > 0 or len(data["files"]) > 0
            ):  # If destination directory is not empty
                # Prompt for yes/no while not get satisfactory answer
                while True:
                    inp = input(
                        f"delete: directory '{destination} is not empty, remove? [y/N]': "
                    ).split()
                    if check_args("delete", tuple(inp), optional_operands=["yes/no"]):
                        if (
                                len(inp) == 0 or inp[0].lower() == "n"
                        ):  # Consider as decline
                            break
                        ans = inp[0]
                        if ans.lower() == "y":  # Consider as accept
                            resp = requests.delete(
                                os.path.join(
                                    MASTER_NODE, f"directory?name={destination}"
                                )
                            )
                            check_response(resp, "delete")
                            break
                        else:
                            print("Incorrect input")
                            continue
            else:
                resp = requests.delete(
                    os.path.join(MASTER_NODE, f"directory?name={destination}")
                )
                check_response(resp, "delete")
        else:
            print(f"delete: cannot remove '{destination}': No such file or directory")


def list_dir(*args):
    if check_args("list", args, optional_operands=["destination"]):
        if len(args) > 1:
            destination = make_abs(args[1])
        else:
            destination = make_abs(".")

        resp = requests.get(os.path.join(MASTER_NODE, f"directory?name={destination}"))
        if not check_response(resp, "list", pretty_print_enabled=True):
            resp = requests.get(
                os.path.join(MASTER_NODE, f"file?filename={destination}")
            )
            if not check_response(resp, "list", pretty_print_enabled=True):
                print(f"list: cannot access '{destination}': No such file or directory")

command_tree = {
    "status": status,
    "copy": copy_file,
    "upload": put_file,
    "cd": change_dir,
    "mkdir": make_dir,
    "download": read_file,
    "delete": remove_file_or_dir,
    "list": list_dir,
}
def main():
    print("Commands and arguments:\n"
          "status : Check status of the YADFS\n"
          "copy <file> <target> : Copy a file to a destination\n"
          "upload <file> <destination> : Upload a local file to the DFS\n"
          "cd <destination> : Change the current directory\n"
          "mkdir <directory> : Create a directory\n"
          "download <file> <local_destination> : Download a file from DFS to local\n"
          "list [directory] : List files in a directory\n"
          "delete <directory> : Delete a directory\n")

    while True:
        args = input(get_pwd() + "@").split()
        if len(args) == 0:
            continue
        try:
            command_tree[args[0]](*args)
        except KeyError:
            print(f"No such command '{args[0]}', please try again")
        except Exception as e:
            print("Command failed, please try again")

main()
