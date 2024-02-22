from google.cloud import storage
import google.auth
import os

class FileStorageFactory:

	@staticmethod
	def create(storage_options=None):
		if storage_options is None:
			return LocalStorage(storage_options)
		elif storage_options.get('GOOGLE_APPLICATION_CREDENTIALS') is not None:
			return GCStorage(storage_options)
		elif storage_opitions.get('AWS_ACCESS_KEY_ID') is not None:
			return S3Storage(storage_options)
		else:
			raise ValueError('storage provider not found')

	def __init__(self):
		pass

class FileStorage:

	def __init__(self, storage_options=None):
		self.storage_options = storage_options
	

class LocalStorage(FileStorage):

	def __init__(self, storage_options=None):
		super().__init__(storage_options)

class S3Storage(FileStorage):
	
	def __init__(self):
		super().__init__(storage_options)

	def download(self, src_path, dest_path):
		if not src_path.startswith("s3://"):
			raise ValueError('filepath is not begin with s3://')
			
		l = src_path[5:].split('/', 1)

class GCStorage(FileStorage):

	def __init__(self, storage_options=None):
		super().__init__(storage_options)

	def download(self, src_path, dest_path):
		if not src_path.startswith("gs://"):
			raise ValueError('filepath is not begin with gs://')

		l = src_path[5:].split('/', 1)
		bucket_name = l[0]
		blob_name = l[1]
		client = storage.Client()
		bucket = client.bucket(bucket_name)
		blob = bucket.blob(blob_name)
		blob.download_to_filename(dest_path)
		
	def upload(self, src_path, dest_path):
		if not dest_path.startswith("gs://"):
			raise ValueError('filepath is not begin with gs://')

		l = dest_path[5:].split('/', 1)
		bucket_name = l[0]
		blob_name = l[1]
		client = storage.Client()
		bucket = client.bucket(bucket_name)
		blob = bucket.blob(blob_name)

		# Optional: set a generation-match precondition to avoid potential race conditions
		# and data corruptions. The request to upload is aborted if the object's
		# generation number does not match your precondition. For a destination
		# object that does not yet exist, set the if_generation_match precondition to 0.
		# If the destination object already exists in your bucket, set instead a
		# generation-match precondition using its generation number.
		generation_match_precondition = 0
		blob.upload_from_filename(src_path, if_generation_match=generation_match_precondition)

	def glob(self, prefix, delimiter=None):
		if not prefix.startswith("gs://"):
			raise ValueError('filepath is not begin with gs://')
			
		l =  prefix[5:].split('/', 1)
		bucket_name = l[0]
		blob_name = l[1]

		client = storage.Client()
		blobs = client.list_blobs(bucket_name, prefix=blob_name, delimiter = delimiter)

		print("blobs")
		for blob in blobs:
			print(blob.name, ", updated = ", blob.updated, ", gen=", blob.generation, ", etag=", blob.etag)

		if delimiter:
			print('prefixes:')
			for p in blobs.prefixes:
				print(p)

	def rename(self, src_path, dest_path):
		if not src_path.startswith("gs://"):
			raise ValueError('filepath is not begin with gs://')
		if not dest_path.startswith("gs://"):
			raise ValueError('filepath is not begin with gs://')
			
		s = src_path[5:].split('/', 1)
		d = dest_path[5:].split('/', 1)
		if s[0] != d[0]:
			raise ValueError('rename must be in same bucket')

		bucket_name = s[0]
		src_blob = s[1]
		dest_blob = d[1]
		client = storage.Client()
		bucket = client.bucket(bucket_name)
		blob = bucket.blob(src_blob)
		new_blob = bucket.rename_blob(blob, dest_blob)

		
	
if __name__ == '__main__':
	
	storage_options = {'GOOGLE_APPLICATION_CREDENTIALS': os.path.join(os.environ.get('HOME'), '.google.json')}
	fs = FileStorageFactory.create(storage_options)

	#fs.glob('gs://vitesse_deltalake/db/serverless/', delimiter='/')
	fs.glob('gs://vitesse_deltalake/db/serverless/')
