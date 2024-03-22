from kitevectorserverless.storage import FileStorageFactory
import os
import tempfile

if __name__ == '__main__':
    
    storage_options = {'GOOGLE_APPLICATION_CREDENTIALS': os.path.join(os.environ.get('HOME'), '.google.json')}
    gsfs = FileStorageFactory.create(storage_options)

    dirs = gsfs.listdir('gs://vitesse_deltalake/db/serverless/')
    print(dirs)
    #fs.list('gs://vitesse_deltalake/db/serverless/')

    localfs = FileStorageFactory.create()
    dirs = localfs.listdir('vitesse/db')
    print(dirs)


    exists = gsfs.exists('gs://vitesse_deltalake/db/serverless/default/6-a0bb2b44-529a-47dd-b8b3-d6a7fb769d0a-0.parquet')
    gsfs.list('gs://vitesse_deltalake/db/serverless/', delimiter='delta_log')
    gsfs.list('gs://vitesse_deltalake/db/serverless/')

    print(exists)


    with tempfile.NamedTemporaryFile(delete=False) as fp:
        fp.close()
        gsfs.download('gs://vitesse_deltalake/db/serverless/default/6-a0bb2b44-529a-47dd-b8b3-d6a7fb769d0a-0.parquet', fp.name)
        print(fp.name)
