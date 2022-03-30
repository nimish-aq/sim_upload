from config import sftp_server_configs, remoteFilePath

import pysftp

Hostname = sftp_server_configs["Hostname"]
Username = sftp_server_configs["Username"]
Password= sftp_server_configs["Password"]
Port = 22
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None



def sftp_file_transfer():
    with pysftp.Connection(host=Hostname, port = Port, username=Username, password=Password, cnopts=cnopts) as sftp:
        print("Connection successfully established ... ")
        try:
          sftp.get(remoteFilePath["remote_file_path"])
        except Exception as e:
            print(e)

    sftp.close()