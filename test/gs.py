from kitevectorserverless.storage import FileStorageFactory
import os

if __name__ == '__main__':
	
	storage_options = {'GOOGLE_APPLICATION_CREDENTIALS': os.path.join(os.environ.get('HOME'), '.google.json')}
	fs = FileStorageFactory.create(storage_options)

	dirs = fs.listdir('gs://vitesse_deltalake/db/serverless/')
	print(dirs)
	#fs.list('gs://vitesse_deltalake/db/serverless/')

	localfs = FileStorageFactory.create()
	dirs = localfs.listdir('vitesse/db')
	print(dirs)




