# Program Name	: Solaris Unix Server API Automation
# Version       : 1.0
# Date          : 26 Aug 2021
# Developer     : Mohamed Abdel-Gawad Ibrahim
# Email         : muhammadabdelgawwad@gmail.com


# Pandas is an open source data analysis and manipulation package
import pandas as pd

# Paramiko is a Python implementation of the SSHv2 protocol,
# providing both client and serv functionality.
import paramiko


class RemoteServer:
	"""A class for connecting to a remote serv and downloading the latest file
	from a specific directory.
	"""
	
	def __init__(self, tag):
		"""Instantiate a RemoteServer object.

		Args:
			tag (str): A string to mark each object of the class.
		"""
		
		# Create a paramiko SSH client
		self.ssh = paramiko.SSHClient()
		
		# Set the policy
		self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		
		# Set the tag attribute
		self.tag = tag
	
	def set_ftp_ip(self, serv_ip):
		"""Set the serv IP attribute.

		Args:
			serv_ip (str): The IP of the remote serv.
		"""
		
		self.serv_ip = serv_ip
	
	def set_ftp_username(self, username):
		"""Set the username of the remote serv.

		Args:
			username (str): The username of the remote serv.
		"""
		
		self.username = username
	
	def set_ftp_password(self, password):
		"""Set the password of the remote serv.

		Args:
			password (str): The password of the remote serv.
		"""
		
		self.password = password
	
	def set_ftp_port(self, port):
		"""Set the port number of the remote serv.

		Args:
			port (str): The port of the remote serv.
		"""
		
		self.port = port
	
	def set_ftp_path(self, path):
		"""Set path of the desired file to download.

		Args:
			path (str): The path of the desired file to download.
		"""
		
		self.path = path
	
	def set_ftp_filename_pattern(self, filename_pattern):
		"""Set The name pattern of the desired file to download.

		Args:
			filename_pattern (str): The name pattern of the desired file
				to download. (Similar to 'servgrpToPhysicalMapping_').
		"""
		
		self.filename_pattern = filename_pattern
	
	def ftp_connect(self):
		"""Make an FTP connection with the remote serv.
		"""
		
		# connect to the remote serv
		self.ssh.connect(hostname=self.serv_ip, username=self.username, password=self.password, port=self.port)
		
		# open  SFTP connection with the remote serv
		self.sftp_client = self.ssh.open_sftp()
		
		# change directory on the remote serv to the path attribute
		self.sftp_client.chdir(self.path)
	
	def get_ftp_latest_filename(self):
		"""Get the Latest file name (based on it timestamp).
		"""
		
		# sort the files in the remote serv from the newest to the oldest,
		# and loop over each file
		for f in sorted(self.sftp_client.listdir_attr(), key=lambda k: k.st_mtime, reverse=True):
			
			# Check if the file name starts with our name pattern,
			# save it in the file_name attribute, the quit the for loop.
			if f.filename.startswith(self.filename_pattern):
				self.file_name = f.filename
				break
	
	def download_ftp_raw_file(self, directory_path):
		"""download the file locally in a specific path.

		Args:
			directory_path (str): The directory to store the file inside it.
		"""
		
		# If tag attribute is set, it will append it to the start
		# of the path
		if hasattr(self, 'tag'):
			path = directory_path + self.tag + "_" + self.file_name
		
		# If tag attribute is not set, it will just use the file name
		else:
			path = directory_path + self.file_name
		
		# download the file
		self.sftp_client.get(self.file_name, path)
	
	def save_ftp_to_csv(self, directory_path):
		"""Save the dataset retrieved from the file as a .csv file in
		a specific directory.

		Args:
			directory_path (str): The directory to store the file inside it.
		"""
		
		# If tag attribute is set, it will append it to the start
		# of the path
		# We remove the last four characters of the filename to remove its
		# extension; as we will save it in .csv format.
		if hasattr(self, 'tag'):
			path = directory_path + self.tag + "_" + self.file_name[:-4] + ".csv"
		
		# If tag attribute is not set, it will just use the file name
		else:
			path = directory_path + self.file_name[:-4] + ".csv"
		
		# save the file in the same directory of this program,
		# and disable the index column that pandas creates
		self.df.to_csv(path, index=False)
	
	def ftp_download_to_df(self, sep_string, cols_list, is_c_engine):
		"""Download the FTP file into a dataframe.

		Args:
			sep_string (str): The separation string between the columns in
				the file. It could be ';' or ','...
			cols_list (List): A list of the columns names in order to create
				the DataFrame.
			is_c_engine (bool): True for using The C-engine to convert the file
				into a DataFrame, and False for Using the Python-engine.
				pandas uses the C parser (specified as engine='c'),
				but may fall back to Python if C-unsupported options
				are specified.
		"""
		
		# set the engine_name to 'c' or 'python'
		if is_c_engine:
			engine_name = 'c'
		else:
			engine_name = 'python'
		
		# open the file and save it inside the DataFrame attribute.
		with self.sftp_client.open(self.file_name) as file:
			self.df = pd.read_csv(file, sep=sep_string, names=cols_list, engine=engine_name)
	
	def close_ftp_connection(self):
		"""Close the connection with the remote serv
		"""
		
		self.ssh.close()
